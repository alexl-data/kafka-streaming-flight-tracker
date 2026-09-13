"""
Producer that polls the OpenSky Network REST API for live aircraft state
vectors over a bounding box (DACH region: Germany, Austria, Switzerland)
and publishes each one as a JSON event to the `flights.raw` Kafka topic.

Data source: https://opensky-network.org/api/states/all (anonymous access,
no authentication required). Anonymous access is capped at 400 API credits
per day and only returns the current snapshot (no historical lookback) -
fine for short demo runs, not for a permanently running service. A free
OpenSky account (OAuth2) raises that quota, but isn't needed to see this
work.

Each aircraft's position is bucketed into one of four coarse quadrants of
the bounding box (nw/ne/sw/se) and used as the Kafka partition key - a real
stand-in for the random `region` key used in produce_demo.py.
"""

import json
import time

import requests
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "flights.raw"
OPENSKY_URL = "https://opensky-network.org/api/states/all"

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


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

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

                region = region_for(record["latitude"], record["longitude"])
                producer.produce(
                    topic=TOPIC,
                    key=region,
                    value=json.dumps(record),
                    callback=delivery_report,
                )
                sent += 1

            producer.poll(0)
            print(f"Polled {len(states)} aircraft, published {sent} events")
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
