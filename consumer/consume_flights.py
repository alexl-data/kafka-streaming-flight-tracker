"""
Consumer for the real flight data pipeline. Reads Avro-serialized events
from `flights.raw` and deserializes them using the schema registered in
Schema Registry.

Unlike the producer side (flights_producer.py), this consumer does NOT
need to know the schema in advance: each Avro message is prefixed with a
5-byte header containing a schema ID, and AvroDeserializer uses that ID to
fetch the matching schema from Schema Registry automatically. This is the
Schema Registry integration that plain open-source Spark does NOT provide
out of the box (see README, Phase 4) - confluent-kafka's Python client
handles it for us here.
"""

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "flights.raw"
GROUP_ID = "flights-consumer-group"


def build_consumer():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer = AvroDeserializer(schema_registry_client)
    return DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": avro_deserializer,
            "auto.offset.reset": "earliest",
        }
    )


def main():
    consumer = build_consumer()
    consumer.subscribe([TOPIC])

    try:
        while True:
            try:
                msg = consumer.poll(timeout=1.0)
            except Exception as exc:
                # DeserializingConsumer raises on decode failure instead of
                # returning an error message (unlike the plain Consumer in
                # consume_demo.py) - a preview of the problem Phase 3's
                # dead-letter topic will handle properly.
                print(f"Deserialization error: {exc}")
                continue

            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            record = msg.value()  # already a dict, AvroDeserializer did the decoding
            print(
                f"partition={msg.partition()} offset={msg.offset()} "
                f"key={msg.key()} icao24={record['icao24']} "
                f"callsign={record['callsign']!r} region={record['region']} "
                f"lat={record['latitude']} lon={record['longitude']}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
