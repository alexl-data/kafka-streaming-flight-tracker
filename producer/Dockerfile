# Built from the repo root (see docker-compose.yml), not from producer/ -
# the producer reads schemas/*.avsc, which sits outside this folder.
FROM python:3.13-slim

# Makes print() output show up in `docker compose logs` immediately.
# Without it, Python collects print() output in a buffer when it isn't
# running in a terminal (as inside a container) and only writes it out
# in occasional batches.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Dependencies before code: this layer only rebuilds when
# requirements.txt changes, not on every edit to the producer itself.
COPY producer/requirements.txt producer/requirements.txt
RUN pip install --no-cache-dir -r producer/requirements.txt

# Same relative layout as the repo, so flights_producer.py's
# Path(__file__).parent.parent / "schemas" resolves unchanged.
COPY schemas/ schemas/
COPY producer/flights_producer.py producer/

# `docker stop` normally sends SIGTERM, which Python ignores when it's the
# container's main process - Docker would then force-kill it after 10s,
# losing events still buffered in the producer. SIGINT is the same signal
# as Ctrl+C: Python turns it into KeyboardInterrupt, so the producer's
# finally block still flushes before exiting.
STOPSIGNAL SIGINT

CMD ["python", "producer/flights_producer.py"]
