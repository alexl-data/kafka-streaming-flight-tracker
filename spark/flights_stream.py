"""
Spark Structured Streaming job: reads Avro-serialized events from
`flights.raw`, aggregates flight counts and average altitude per region
in 1-minute tumbling windows, and prints the running results to the
console. A real sink (ClickHouse) comes in Phase 6.

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

The checkpoint directory is separate from the earlier unwindowed version
of this job: a checkpoint is tied to the shape of the specific streaming
query that created it (stateless passthrough vs. stateful windowed
aggregation are different query plans), not just to the source topic, so
reusing the old checkpoint here would fail to resume correctly.
"""

import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import avg, coalesce, col, count, expr, window

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "flights.raw"
CHECKPOINT_LOCATION = "checkpoints/flights_windowed"
WINDOW_DURATION = "1 minute"
WATERMARK_DELAY = "1 minute"


def fetch_latest_schema(subject):
    response = requests.get(
        f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=10
    )
    response.raise_for_status()
    return response.json()["schema"]


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
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("region"),
            col("flight_count"),
            col("avg_altitude_m"),
        )
    )

    # "update" instead of "append": prints a window's aggregate as it
    # changes with each new batch, rather than waiting for the watermark
    # to close the window before showing anything.
    query = (
        windowed.writeStream.format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
