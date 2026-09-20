"""
Producer that polls the OpenSky Network REST API for live aircraft state
vectors over a bounding box (DACH region: Germany, Austria, Switzerland)
and publishes each one as an Avro-serialized event to the `flights.raw`
Kafka topic, using the schema in schemas/flight_state.avsc registered in
Schema Registry.

Data source: https://opensky-network.org/api/states/all (anonymous access,
no authentication required). Anonymous access is capped at 400 API credits
per day and only returns the current snapshot (no historical lookback) -
fine for short demo runs, not for a permanently running service. A free
OpenSky account (OAuth2) raises that quota, but isn't needed to see this
work.

Each aircraft's position is bucketed into one of four coarse quadrants of
the bounding box (nw/ne/sw/se) and used as the Kafka partition key - a real
stand-in for the random `region` key used in produce_demo.py.

A record that fails Avro serialization (e.g. a required field missing
because OpenSky's response was incomplete) is routed to the
`flights.raw.dlq` topic instead of crashing the producer, wrapped in the
same DlqEnvelope schema (schemas/dlq_envelope.avsc) that
spark/flights_stream.py uses for its own failures - one consistent shape
for the DLQ topic, regardless of which stage caught the problem.
"""

import json
import time
from pathlib import Path

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.error import KeySerializationError, ValueSerializationError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

BOOTSTRAP_SERVERS = "localhost:9092"
# Host-side port for Schema Registry - see the port comment in
# docker-compose.yml for why it's 8082 instead of the 8081 default.
SCHEMA_REGISTRY_URL = "http://localhost:8082"
TOPIC = "flights.raw"
DLQ_TOPIC = "flights.raw.dlq"
OPENSKY_URL = "https://opensky-network.org/api/states/all"
SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "schemas"
SCHEMA_PATH = SCHEMAS_DIR / "flight_state.avsc"
DLQ_SCHEMA_PATH = SCHEMAS_DIR / "dlq_envelope.avsc"

# Bounding box covering Germany, Austria, and Switzerland (DACH region).
BBOX = {"lamin": 45.8, "lomin": 5.9, "lamax": 55.1, "lomax": 17.2}
LAT_MID = (BBOX["lamin"] + BBOX["lamax"]) / 2
LON_MID = (BBOX["lomin"] + BBOX["lomax"]) / 2

# Anonymous access allows 400 credits/day. Polling every 15s would already
# exhaust that within roughly 1.5-2 hours - acceptable for a short demo
# run, not for continuous operation.
POLL_INTERVAL_SECONDS = 15

# Index positions within each state vector array, per the OpenSky REST API
# spec: https://openskynetwork.github.io/opensky-api/rest.html
FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]


def region_for(lat, lon):
    """Buckets a position into one of four quadrants of the bounding box."""
    ns = "n" if lat >= LAT_MID else "s"
    ew = "e" if lon >= LON_MID else "w"
    return ns + ew


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")


def fetch_states():
    response = requests.get(OPENSKY_URL, params=BBOX, timeout=10)
    response.raise_for_status()
    return response.json().get("states") or []


def build_producer(schema_registry_client):
    # AvroSerializer needs the writer schema up front (unlike the read
    # side, where AvroDeserializer resolves the schema dynamically by ID -
    # see spark/flights_stream.py's docstring for why Spark can't do that
    # automatically) and registers it with Schema Registry on first use if
    # it isn't already registered there.
    avro_serializer = AvroSerializer(
        schema_registry_client,
        SCHEMA_PATH.read_text(),
    )
    return SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )


def build_dlq_producer(schema_registry_client):
    # Same envelope schema spark/flights_stream.py's DLQ path uses - see
    # schemas/dlq_envelope.avsc. One shared SchemaRegistryClient instance
    # is fine to reuse across serializers for different schemas/subjects.
    avro_serializer = AvroSerializer(schema_registry_client, DLQ_SCHEMA_PATH.read_text())
    return SerializingProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "value.serializer": avro_serializer,
        }
    )


def send_to_dlq(dlq_producer, record, exc):
    envelope = {
        "stage": "producer",
        "error": str(exc),
        "error_type": type(exc).__name__,
        "source_topic": TOPIC,
        "source_partition": None,  # never reached a partition - produce() failed
        "source_offset": None,
        "failed_at": int(time.time() * 1000),
        "original_key": record.get("region"),
        # The record was never successfully Avro-serialized, so its JSON
        # form is the closest thing to "the original payload" we have.
        "original_value": json.dumps(record, default=str).encode("utf-8"),
    }
    dlq_producer.produce(topic=DLQ_TOPIC, value=envelope)


def main():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    producer = build_producer(schema_registry_client)
    dlq_producer = build_dlq_producer(schema_registry_client)

    try:
        while True:
            try:
                states = fetch_states()
            except requests.RequestException as exc:
                print(f"Failed to fetch state vectors: {exc}")
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            sent = 0
            for state in states:
                record = dict(zip(FIELDS, state))
                if record["latitude"] is None or record["longitude"] is None:
                    continue  # aircraft currently without a position fix

                record.pop("sensors", None)  # not part of flight_state.avsc
                record["region"] = region_for(record["latitude"], record["longitude"])
                # Schema v2 field (see schemas/flight_state.avsc) - when our
                # producer processed this record, as opposed to OpenSky's
                # own last_contact timestamp.
                record["ingested_at"] = int(time.time() * 1000)

                # SerializingProducer runs `record` through the configured
                # key/value serializers itself - no manual json.dumps() or
                # Avro encoding here, unlike the plain Producer in
                # produce_demo.py. That serialization happens synchronously,
                # right here in produce() - so a malformed record (e.g. a
                # required field missing because OpenSky's response was
                # incomplete) raises immediately, not asynchronously via
                # the delivery callback.
                try:
                    producer.produce(
                        topic=TOPIC,
                        key=record["region"],
                        value=record,
                        on_delivery=delivery_report,
                    )
                    sent += 1
                except (ValueSerializationError, KeySerializationError) as exc:
                    print(f"Serialization failed, routing to DLQ: {exc}")
                    send_to_dlq(dlq_producer, record, exc)

            producer.poll(0)
            dlq_producer.poll(0)
            print(f"Polled {len(states)} aircraft, published {sent} events")
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        dlq_producer.flush()


if __name__ == "__main__":
    main()
