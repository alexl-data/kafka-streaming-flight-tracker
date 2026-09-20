"""
Spark Structured Streaming job: reads Avro-serialized events from
`flights.raw`, aggregates flight count, average altitude, and average
speed per region in 1-minute tumbling windows, and writes the running
results to ClickHouse (table `region_traffic`, see clickhouse/init.sql).

Must be launched with spark-submit, not plain `python`, so the Kafka and
Avro connector packages (and their transitive dependencies) get resolved
onto the classpath - see README for the exact command.

Open-source Spark has no built-in Schema Registry integration (unlike
Databricks Runtime's `from_avro` variant, which can talk to a registry
directly). Here the schema is fetched once at startup via Schema
Registry's REST API, and Confluent's 5-byte wire-format header (1 magic
byte + 4-byte schema ID) is stripped from each message before Spark's own
`from_avro()` - which knows nothing about that header - can parse it.

Reads `startingOffsets: latest` rather than `earliest`: `from_avro()`
decodes against one fixed schema, and `flights.raw` has a mix of schema v1
and v2 messages (see README, Schema Evolution) that a single schema can't
both decode. Starting from "latest" reads only messages produced under the
current schema.

This job is invisible to standard consumer-lag tooling (Kafka UI,
kafka-consumer-groups.sh) - verified against Spark's own docs and by
testing: Spark's Kafka source never commits offsets to Kafka's consumer
group coordinator, regardless of whether `kafka.group.id` is set (that
option exists only for authorization/ACL scenarios, and Spark's docs warn
against setting it otherwise - a query started with it wasn't visible to
`kafka-consumer-groups --list` either). There's no supported way to make
this job's progress visible through classic consumer-group lag tools; see
README, Monitoring for how progress is tracked instead.

Windowing uses `time_position` (when OpenSky recorded the position) as
event time, not `ingested_at` (when our producer processed the record) -
the two diverge whenever a batch arrives late or out of order, which is
exactly what the watermark below needs to reason about.

Each event is also left-joined against a small static lookup table of
standardized ICAO emergency squawk codes (7500 hijacking, 7600 radio
failure, 7700 general emergency) - a stream-static join, Spark's term for
joining a streaming DataFrame against a small, in-memory, non-streaming
one. Non-matching rows (nearly all of them - the vast majority of flights
never squawk an emergency code) simply get a null `alert_type`, counted
separately per window/region as `emergency_count` alongside the existing
aggregates.

ClickHouse has no native Spark Structured Streaming connector, so writes
go through `foreachBatch`, Spark's escape hatch for sinks without one:
Spark hands each micro-batch to a plain Python function instead of a
built-in writer. The table itself (ReplacingMergeTree, see
clickhouse/init.sql) is what makes re-inserting the same batch after a
retry idempotent - ClickHouse collapses rows with the same
(window_start, region) down to the most recent one, so a replayed batch
doesn't duplicate data even though `foreachBatch` itself gives no
exactly-once guarantee on its own.

Every message is also checked for a valid Confluent wire-format header
(1 magic byte, must be 0x00, + 4-byte schema ID) before from_avro() ever
sees it. This mirrors a check confluent-kafka's Python AvroDeserializer
already does internally for us (confirmed earlier by testing it against
deliberately invalid bytes with a plain Kafka consumer, which raised
"Invalid magic byte") - Spark's from_avro() has no equivalent built in.
Verified empirically before relying on it: from_avro() with option
"mode": "PERMISSIVE" does NOT reliably flag malformed input as null/error
- fed deliberately garbage bytes, it silently decoded them into a
plausible-looking-but-wrong value instead of failing, because Avro's
binary format has no checksum to validate against and a simple schema can
make almost any bytes "decode" into something. Messages failing the
header check are routed to `flights.raw.dlq` directly (skipping
from_avro entirely) using the same DlqEnvelope schema flights_producer.py
uses for its own failures - deliberately scoped to only this one failure
class (no Confluent header at all); a valid header with corrupted payload
bytes after it would not be caught by this check.
"""

import time
from pathlib import Path

import clickhouse_connect
import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    current_timestamp,
    expr,
    length,
    lit,
    when,
    window,
)

# Static reference data for the stream-static join below - standardized
# ICAO emergency transponder (squawk) codes, not derived from any stream.
EMERGENCY_SQUAWK_CODES = [
    ("7500", "hijacking"),
    ("7600", "radio_failure"),
    ("7700", "general_emergency"),
]

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "flights.raw"
DLQ_TOPIC = "flights.raw.dlq"
DLQ_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "dlq_envelope.avsc"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_TABLE = "region_traffic"
CHECKPOINT_LOCATION = "checkpoints/region_traffic"
DLQ_CHECKPOINT_LOCATION = "checkpoints/flights_dlq"
WINDOW_DURATION = "1 minute"
WATERMARK_DELAY = "1 minute"


def fetch_latest_schema(subject):
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=10
    )
    response.raise_for_status()
    return response.json()["schema"]


def write_to_clickhouse(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    data = [
        [
            r.window_start,
            r.window_end,
            r.region,
            r.flight_count,
            r.avg_altitude_m,
            r.avg_velocity_kmh,
            r.emergency_count,
            r.updated_at,
        ]
        for r in rows
    ]
    client.insert(
        CLICKHOUSE_TABLE,
        data,
        column_names=[
            "window_start",
            "window_end",
            "region",
            "flight_count",
            "avg_altitude_m",
            "avg_velocity_kmh",
            "emergency_count",
            "updated_at",
        ],
    )
    print(f"Batch {batch_id}: wrote {len(data)} rows to ClickHouse")


def write_bad_to_dlq(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    dlq_producer = SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "value.serializer": AvroSerializer(schema_registry_client, DLQ_SCHEMA_PATH.read_text()),
        }
    )
    for r in rows:
        envelope = {
            "stage": "spark",
            "error": "Missing or invalid Confluent wire-format header (magic byte != 0x00)",
            "error_type": "InvalidMagicByte",
            "source_topic": r.topic,
            "source_partition": r.partition,
            "source_offset": r.offset,
            "failed_at": int(time.time() * 1000),
            "original_key": r.key.decode("utf-8", errors="replace") if r.key is not None else None,
            "original_value": r.value,
        }
        dlq_producer.produce(topic=DLQ_TOPIC, value=envelope)
    dlq_producer.flush()
    print(f"Batch {batch_id}: routed {len(rows)} invalid messages to DLQ")


def main():
    schema_str = fetch_latest_schema(f"{TOPIC}-value")

    spark = SparkSession.builder.appName("flights-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # See module docstring: from_avro() has no way to detect malformed
    # input on its own, so messages without a valid Confluent header are
    # filtered out and routed to the DLQ before from_avro() ever sees them.
    has_valid_header = (length(col("value")) >= 5) & (
        col("value").substr(1, 1) == lit(bytes([0]))
    )
    good = raw.filter(has_valid_header)
    bad = raw.select("key", "value", "topic", "partition", "offset").filter(
        ~has_valid_header
    )

    # substring() is 1-indexed in Spark SQL: byte 6 onward skips the 5-byte
    # Confluent header (1 magic byte + 4-byte schema ID).
    avro_payload = expr("substring(value, 6, length(value) - 5)")

    parsed = good.select(from_avro(avro_payload, schema_str).alias("flight")).select(
        "flight.*"
    )

    emergency_squawks = spark.createDataFrame(
        EMERGENCY_SQUAWK_CODES, ["squawk", "alert_type"]
    )
    # Stream-static join: emergency_squawks never changes and isn't read
    # from Kafka, so this is a plain DataFrame join, not a join between
    # two streaming sources (Structured Streaming has extra restrictions
    # for those). Left join so a normal (non-emergency) squawk - the vast
    # majority - just gets a null alert_type instead of being dropped.
    enriched = parsed.join(emergency_squawks, on="squawk", how="left")

    # time_position can be null (rare, but the schema allows it) - fall
    # back to last_contact rather than dropping the row from windowing.
    events = enriched.withColumn(
        "event_time",
        coalesce(col("time_position"), col("last_contact")).cast("timestamp"),
    ).withWatermark("event_time", WATERMARK_DELAY)

    windowed = (
        events.groupBy(window(col("event_time"), WINDOW_DURATION), col("region"))
        .agg(
            count("*").alias("flight_count"),
            avg("baro_altitude").alias("avg_altitude_m"),
            # Enrichment: OpenSky reports velocity in m/s: convert to
            # km/h, the more common unit for a "traffic" style view.
            avg(col("velocity") * 3.6).alias("avg_velocity_kmh"),
            # From the stream-static join above - counts rows where the
            # join actually matched an emergency code.
            count(when(col("alert_type").isNotNull(), 1)).alias("emergency_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("flight_count"),
            col("avg_altitude_m"),
            col("avg_velocity_kmh"),
            col("emergency_count"),
            # Version column for ClickHouse's ReplacingMergeTree - see
            # clickhouse/init.sql and the module docstring.
            current_timestamp().alias("updated_at"),
        )
    )

    # "update" instead of "append": writes a window's aggregate to
    # ClickHouse as it changes with each new batch, rather than waiting
    # for the watermark to close the window before writing anything.
    windowed.writeStream.foreachBatch(write_to_clickhouse).outputMode(
        "update"
    ).option("checkpointLocation", CHECKPOINT_LOCATION).start()

    # Separate streaming query with its own checkpoint - a different query
    # shape (no aggregation) than the windowed one above, so it can't share
    # a checkpoint with it. Runs concurrently with the query above; both
    # were already started (non-blocking) by the time either await below
    # runs.
    bad.writeStream.foreachBatch(write_bad_to_dlq).outputMode("append").option(
        "checkpointLocation", DLQ_CHECKPOINT_LOCATION
    ).start()

    # Waits for either query to stop (including a crash) rather than only
    # the first one started - awaiting a single query's awaitTermination()
    # would block forever on that one query alone and never notice if the
    # other one failed.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
