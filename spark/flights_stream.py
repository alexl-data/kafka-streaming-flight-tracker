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

Windowing uses `time_position` (when OpenSky recorded the position) as
event time, not `ingested_at` (when our producer processed the record) -
the two diverge whenever a batch arrives late or out of order, which is
exactly what the watermark below needs to reason about.

The checkpoint directory is named after this job's sink/aggregation shape,
not just the source topic: a checkpoint is tied to the exact streaming
query that created it, and both the query (new avg_velocity_kmh column)
and the sink (ClickHouse instead of console) changed again in this phase -
reusing an older checkpoint here would fail to resume correctly.

ClickHouse has no native Spark Structured Streaming connector, so writes
go through `foreachBatch`, Spark's escape hatch for sinks without one:
Spark hands each micro-batch to a plain Python function instead of a
built-in writer. The table itself (ReplacingMergeTree, see
clickhouse/init.sql) is what makes re-inserting the same batch after a
retry idempotent - ClickHouse collapses rows with the same
(window_start, region) down to the most recent one, so a replayed batch
doesn't duplicate data even though `foreachBatch` itself gives no
exactly-once guarantee on its own.
"""

import clickhouse_connect
import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import avg, coalesce, col, count, current_timestamp, expr, window

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "flights.raw"
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_TABLE = "region_traffic"
CHECKPOINT_LOCATION = "checkpoints/flights_clickhouse"
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
            "updated_at",
        ],
    )
    print(f"Batch {batch_id}: wrote {len(data)} rows to ClickHouse")


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

    # substring() is 1-indexed in Spark SQL: byte 6 onward skips the 5-byte
    # Confluent header (1 magic byte + 4-byte schema ID).
    avro_payload = expr("substring(value, 6, length(value) - 5)")

    parsed = raw.select(from_avro(avro_payload, schema_str).alias("flight")).select(
        "flight.*"
    )

    # time_position can be null (rare, but the schema allows it) - fall
    # back to last_contact rather than dropping the row from windowing.
    events = parsed.withColumn(
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
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("flight_count"),
            col("avg_altitude_m"),
            col("avg_velocity_kmh"),
            # Version column for ClickHouse's ReplacingMergeTree - see
            # clickhouse/init.sql and the module docstring.
            current_timestamp().alias("updated_at"),
        )
    )

    # "update" instead of "append": writes a window's aggregate to
    # ClickHouse as it changes with each new batch, rather than waiting
    # for the watermark to close the window before writing anything.
    query = (
        windowed.writeStream.foreachBatch(write_to_clickhouse)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
