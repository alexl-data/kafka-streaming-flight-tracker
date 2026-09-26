"""
CI check: for each schema in schemas/, verifies the current (HEAD) version
is compatible with the version at $BASE_REF (a git ref/SHA, default
HEAD~1), using Schema Registry's own compatibility check - the same check
Schema Registry enforces at registration time (see README, Schema
Evolution). A schema file that doesn't exist at that ref is treated as new and passes
without a check - there's nothing to be incompatible with.
"""

import os
import subprocess
import sys
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

SCHEMA_REGISTRY_URL = "http://localhost:8082"
BASE_REF = os.environ.get("BASE_REF", "HEAD~1")
REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = REPO_ROOT / "schemas"

# Maps each .avsc file to the Schema Registry subject(s) it governs
# (TopicNameStrategy: <topic>-value - see producer/flights_producer.py).
SUBJECTS = {
    "flight_state.avsc": ["flights.raw-value", "flights.latest-value"],
    "dlq_envelope.avsc": ["flights.raw.dlq-value"],
}


def schema_at_ref(relative_path: str, ref: str) -> str | None:
    result = subprocess.run(
        ["git", "show", f"{ref}:{relative_path}"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    return result.stdout if result.returncode == 0 else None


def main() -> int:
    client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    failed = False

    for filename, subjects in SUBJECTS.items():
        path = SCHEMAS_DIR / filename
        current = path.read_text()
        base = schema_at_ref(f"schemas/{filename}", BASE_REF)

        if base is None:
            print(f"{filename}: new file, nothing to compare against - OK")
            continue
        if base == current:
            print(f"{filename}: unchanged - OK")
            continue

        for subject in subjects:
            client.register_schema(subject, Schema(base, "AVRO"))
            compatible = client.test_compatibility(subject, Schema(current, "AVRO"))
            print(f"{filename} -> {subject}: {'OK' if compatible else 'INCOMPATIBLE'}")
            if not compatible:
                failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
