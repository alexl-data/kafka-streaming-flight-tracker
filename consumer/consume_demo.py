"""
Minimal Kafka consumer used to learn the basics: reading messages, consumer
groups, and offset management. Separate from the real pipeline
(`spark/flights_stream.py`, topic `flights.raw`) - see produce_demo.py for
why. Spark's Kafka source doesn't use Kafka's consumer-group rebalancing
protocol at all (it assigns partitions to its own tasks internally), so
this script is the only place in the project that demonstrates it.

Run two instances of this script at once (same GROUP_ID) to see Kafka split
the topic's partitions between them, and reassign them when one instance
stops - that's the consumer group / rebalancing demo described in the
README roadmap.
"""

import json

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "demo.events"
GROUP_ID = "demo-consumer-group"


def on_assign(consumer, partitions):
    """Called whenever Kafka (re-)assigns partitions to this consumer instance.

    Not required for consuming messages - Kafka assigns partitions
    automatically either way. This is only here so the partition split
    between multiple consumer instances is visible on screen for the demo.
    """
    assigned = [p.partition for p in partitions]
    print(f">>> Assigned partitions: {assigned}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            # No committed offset yet for this group? Start from the
            # beginning of the topic instead of only new messages.
            "auto.offset.reset": "earliest",
            # Periodically tell Kafka "I've successfully processed up to
            # here", so a restarted consumer resumes instead of
            # reprocessing or skipping messages. This is Kafka's default;
            # spelled out here so the behavior is explicit.
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([TOPIC], on_assign=on_assign)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            print(
                f"partition={msg.partition()} offset={msg.offset()} "
                f"key={msg.key().decode()} value={event}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        # close() leaves the group immediately, triggering a prompt
        # rebalance for the remaining members instead of making them wait
        # for a session timeout. Note: this only runs on a clean Ctrl+C.
        # A hard kill (SIGTERM, e.g. from `docker stop`) skips this, and
        # Kafka only notices the consumer is gone after the session
        # timeout (45s by default) - a refinement for a later phase, not
        # needed to understand how consumer groups work.
        consumer.close()


if __name__ == "__main__":
    main()
