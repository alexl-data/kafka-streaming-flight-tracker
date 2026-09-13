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

Uses the plain Consumer (not DeserializingConsumer) and calls the Avro
deserializer manually. DeserializingConsumer raises straight out of
poll() on a decode failure, without giving access to the raw undecoded
bytes - which we need here to forward a bad message, untouched, to
`flights.raw.dlq` instead of just logging and dropping it.

DLQ messages are wrapped in the DlqEnvelope schema
(schemas/dlq_envelope.avsc) - the same one flights_producer.py uses for
its own failures, so `flights.raw.dlq` has one consistent shape no matter
which side of the pipeline caught the problem, instead of mixing raw bytes
and JSON in the same topic.
"""

import time
from pathlib import Path

from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "flights.raw"
DLQ_TOPIC = "flights.raw.dlq"
GROUP_ID = "flights-consumer-group"
DLQ_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "dlq_envelope.avsc"


def build_consumer():
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )


def build_dlq_producer(schema_registry_client):
    avro_serializer = AvroSerializer(schema_registry_client, DLQ_SCHEMA_PATH.read_text())
    return SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "value.serializer": avro_serializer,
        }
    )


def send_to_dlq(dlq_producer, msg, exc):
    # msg.value() is forwarded exactly as received - it already failed to
    # decode once, so there's no "corrected" version to send. Only the
    # metadata around it is standardized, via the same envelope shape
    # flights_producer.py uses for its own (differently-caused) failures.
    envelope = {
        "stage": "consumer",
        "error": str(exc),
        "error_type": type(exc).__name__,
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "failed_at": int(time.time() * 1000),
        "original_key": msg.key().decode("utf-8", errors="replace") if msg.key() is not None else None,
        "original_value": msg.value(),
    }
    dlq_producer.produce(topic=DLQ_TOPIC, value=envelope)


def main():
    consumer = build_consumer()
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    dlq_producer = build_dlq_producer(schema_registry_client)
    avro_deserializer = AvroDeserializer(schema_registry_client)
    consumer.subscribe([TOPIC])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            try:
                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                record = avro_deserializer(msg.value(), ctx)
            except Exception as exc:
                # Deliberately broad: this is the pipeline's error
                # boundary for "whatever is wrong with this message" -
                # corrupt bytes, an unknown schema ID, a registry lookup
                # failure. Anything caught here goes to the DLQ instead of
                # crashing the consumer or silently skipping the message.
                print(f"Deserialization failed, routing to DLQ: {exc}")
                send_to_dlq(dlq_producer, msg, exc)
                dlq_producer.poll(0)
                continue

            key = msg.key().decode("utf-8") if msg.key() is not None else None
            print(
                f"partition={msg.partition()} offset={msg.offset()} "
                f"key={key} icao24={record['icao24']} "
                f"callsign={record['callsign']!r} region={record['region']} "
                f"lat={record['latitude']} lon={record['longitude']}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
