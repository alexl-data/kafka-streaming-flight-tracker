"""
Minimal Kafka producer used to learn the basics before wiring up the real
OpenSky data source in a later phase (see README roadmap, Phase 2).

Sends a handful of synthetic events to the `flights.raw` topic, using a
`region` field as the partition key - a stand-in for the real geographic
region we'll derive from actual flight positions later.
"""

import json
import random
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "flights.raw"
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
