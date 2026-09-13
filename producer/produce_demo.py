"""
Minimal Kafka producer used to learn the basics: producing, partitioning,
consumer groups. Separate from the real pipeline (`flights_producer.py`,
topic `flights.raw`) so this reference example never mixes its synthetic
JSON events with the real Avro-serialized flight data.

Sends a handful of synthetic events to the `demo.events` topic, using a
`region` field as the partition key.
"""

import json
import random
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "demo.events"
REGIONS = ["north", "south", "east", "west"]


def delivery_report(err, msg):
    """Called asynchronously once Kafka has acknowledged (or failed) a message."""
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
    else:
        print(
            f"Delivered key={msg.key().decode()} to partition "
            f"{msg.partition()} at offset {msg.offset()}"
        )


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for i in range(20):
        region = random.choice(REGIONS)
        event = {"id": i, "region": region, "timestamp": time.time()}

        # produce() is asynchronous: it queues the message and returns
        # immediately. The actual send happens in the background; poll()
        # below is what triggers delivery_report callbacks for messages
        # that have completed sending by that point.
        producer.produce(
            topic=TOPIC,
            key=region,
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.5)

    # flush() blocks until every queued message has actually been sent and
    # acknowledged. Without this, the script could exit before the last
    # few messages ever leave the process.
    producer.flush()


if __name__ == "__main__":
    main()
