"""
Live map web app: shows every aircraft's current position on a map, next
to recent per-region traffic - the two read paths of this pipeline side by
side:

- Current state from Kafka: a background thread reads `flights.latest`
  (compacted, one record per icao24) from the beginning and keeps the
  latest position per aircraft in memory -> /api/flights
- History from ClickHouse: the Gold-layer aggregates Spark writes to
  `region_traffic` -> /api/regions

static/index.html polls both endpoints and renders them (Leaflet map +
table). See README, Live Map.
"""

import os
import threading
import time
import uuid

import clickhouse_connect
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from flask import Flask, jsonify

# Host-side defaults, overridden with in-network addresses by
# docker-compose.yml - see the same comment in flights_producer.py.
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8082")
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = 8123
TOPIC = "flights.latest"
PORT = 5000

# flights.latest keeps an aircraft's last position forever, even long after
# it landed or left the bounding box - only aircraft heard from within this
# window are shown as "live".
MAX_AGE_SECONDS = 120
# How many 1-minute windows /api/regions returns per region.
REGION_WINDOWS = 10

app = Flask(__name__, static_folder="static", static_url_path="")

# The latest known record of every aircraft, keyed by its icao24 - one entry
# per aircraft, overwritten whenever a newer record for it arrives (the
# in-memory equivalent of the compacted topic). Filled by the Kafka
# consumer thread, read by the Flask request handlers; the lock keeps a
# request from reading it while the consumer thread is changing it.
positions = {}
positions_lock = threading.Lock()


def consume_latest():
    deserializer = AvroDeserializer(SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}))
    consumer = DeserializingConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # A fresh group id on every start, and no offset commits: the
            # app always re-reads the whole compacted topic from the
            # beginning, which rebuilds the current state of every
            # aircraft - instead of resuming where a previous run stopped
            # and missing everything before that.
            "group.id": f"live-map-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "key.deserializer": StringDeserializer("utf_8"),
            "value.deserializer": deserializer,
        }
    )
    consumer.subscribe([TOPIC])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        with positions_lock:
            positions[msg.key()] = msg.value()


@app.get("/")
def index():
    return app.send_static_file("index.html")


@app.get("/api/flights")
def flights():
    cutoff = time.time() - MAX_AGE_SECONDS
    with positions_lock:
        records = list(positions.values())
    live = [
        {
            "icao24": r["icao24"],
            "callsign": (r["callsign"] or "").strip(),
            "origin_country": r["origin_country"],
            "latitude": r["latitude"],
            "longitude": r["longitude"],
            "altitude_m": r["baro_altitude"],
            "velocity_kmh": r["velocity"] * 3.6 if r["velocity"] is not None else None,
            "true_track": r["true_track"],
            "on_ground": r["on_ground"],
            "squawk": r["squawk"],
        }
        for r in records
        if (r["last_contact"] or 0) >= cutoff
    ]
    return jsonify(live)


@app.get("/api/regions")
def regions():
    client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)
    # FINAL collapses the multiple versions of a still-open window that
    # Spark's update mode writes into one row each - see README,
    # ClickHouse Sink.
    result = client.query(
        f"""
        SELECT window_start, region, flight_count, avg_altitude_m,
               avg_velocity_kmh, emergency_count
        FROM region_traffic FINAL
        WHERE window_start >= (SELECT max(window_start) FROM region_traffic)
                              - INTERVAL {REGION_WINDOWS - 1} MINUTE
        ORDER BY window_start DESC, region
        """
    )
    rows = [
        {
            "window_start": r[0].isoformat(),
            "region": r[1],
            "flight_count": r[2],
            "avg_altitude_m": r[3],
            "avg_velocity_kmh": r[4],
            "emergency_count": r[5],
        }
        for r in result.result_rows
    ]
    return jsonify(rows)


if __name__ == "__main__":
    # daemon=True: the thread doesn't keep the process alive on shutdown.
    threading.Thread(target=consume_latest, daemon=True).start()
    # Flask's built-in server - fine for a single-user demo; a production
    # deployment would put it behind a WSGI server such as gunicorn.
    app.run(host="0.0.0.0", port=PORT)
