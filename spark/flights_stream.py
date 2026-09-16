"""
First Spark Structured Streaming job: reads Avro-serialized events from
`flights.raw` and prints them to the console.

Scope is deliberately narrow - Kafka source, Avro deserialization, and
checkpointing only. Windowed aggregations come in Phase 5, a real sink
(ClickHouse) in Phase 6.

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
"""

import requests
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import expr

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "flights.raw"
CHECKPOINT_LOCATION = "checkpoints/flights_stream"


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

    query = (
        parsed.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
