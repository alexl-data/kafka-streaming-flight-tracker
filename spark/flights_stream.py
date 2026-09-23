"""
Spark Structured Streaming job: reads Avro-serialized events from
`flights.raw`, joins them against a static table of emergency squawk
codes, and aggregates flight count, average altitude, average speed, and
emergency-squawk count per region in 1-minute tumbling windows, written
to ClickHouse (table `region_traffic`, see clickhouse/init.sql).

Must be launched with spark-submit, not plain `python`, so the Kafka and
Avro connector packages resolve onto the classpath - see README for the
exact command.

See README (Spark Structured Streaming, Error Handling, Monitoring) for
the rationale behind the manual Schema Registry fetch, the magic-byte
DLQ check, `startingOffsets: latest`, and the consumer-lag tooling gap.
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
from pyspark.sql.streaming import StreamingQueryListener

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
# Matches flights_producer.py's own poll interval (POLL_INTERVAL_SECONDS) -
# checking for new data more often than the producer can possibly supply
# it just adds empty micro-batch overhead without reducing latency.
TRIGGER_INTERVAL = "15 seconds"


def fetch_latest_schema(subject):
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=10
    )
    response.raise_for_status()
    return response.json()["schema"]


def write_to_clickhouse(batch_df, batch_id):
    # collect() pulls the batch to the driver rather than writing per
    # partition on the executors - fine here since batch_df is already the
    # aggregated result (a handful of rows per region/window), not raw
    # events; foreachPartition would be the right call at larger volumes.
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


class ProgressLogger(StreamingQueryListener):
    """
    Spark's own operational metrics for both queries (throughput, batch
    duration) - the replacement for classic Kafka consumer-lag tooling,
    which doesn't work here (see README, Monitoring). Registered once via
    spark.streams.addListener(); onQueryProgress fires after every
    completed micro-batch for every active query in this session.
    """

    def onQueryStarted(self, event):
        print(f"[{event.name}] started")

    def onQueryProgress(self, event):
        p = event.progress
        print(
            f"[{p.name}] batch={p.batchId} rows={p.numInputRows} "
            f"rows/sec={p.inputRowsPerSecond:.2f} batch_duration_ms={p.batchDuration}"
        )

    def onQueryTerminated(self, event):
        print(f"[query {event.id}] terminated" + (f": {event.exception}" if event.exception else ""))


def main():
    schema_str = fetch_latest_schema(f"{TOPIC}-value")

    spark = SparkSession.builder.appName("flights-stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressLogger())

    # "latest", not "earliest": from_avro() below decodes against one fixed
    # schema fetched at startup, and flights.raw holds records written both
    # before and after the schema last changed - a single schema can't
    # decode both (see README, Schema Evolution).
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
    # Stream-static join: emergency_squawks is a small in-memory table, not
    # read from Kafka, so this is an ordinary join - not a stream-stream
    # join, which Structured Streaming restricts far more heavily. Left
    # join, since most squawks aren't emergency codes and should just get
    # a null alert_type rather than being dropped.
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
    # queryName() gives ProgressLogger something readable to print instead
    # of a raw UUID; trigger() caps how often Spark even checks for new
    # data - see TRIGGER_INTERVAL above.
    windowed.writeStream.foreachBatch(write_to_clickhouse).outputMode(
        "update"
    ).queryName("region_traffic").trigger(processingTime=TRIGGER_INTERVAL).option(
        "checkpointLocation", CHECKPOINT_LOCATION
    ).start()

    # Separate streaming query with its own checkpoint - a different query
    # shape (no aggregation) than the windowed one above, so it can't share
    # a checkpoint with it. Runs concurrently with the query above; both
    # are already started (non-blocking) by the time the await below runs.
    bad.writeStream.foreachBatch(write_bad_to_dlq).outputMode("append").queryName(
        "flights_dlq"
    ).trigger(processingTime=TRIGGER_INTERVAL).option(
        "checkpointLocation", DLQ_CHECKPOINT_LOCATION
    ).start()

    # Waits for either query to stop (including a crash) rather than only
    # the first one started - awaiting a single query's awaitTermination()
    # would block forever on that one query alone and never notice if the
    # other one failed.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
