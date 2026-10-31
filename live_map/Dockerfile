# Built from the repo root, like producer/Dockerfile and spark/Dockerfile -
# keeps all three image builds configured the same way in
# docker-compose.yml and cd.yml.
FROM python:3.13-slim

# Makes print() output show up in `docker compose logs` immediately - see
# producer/Dockerfile.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY live_map/requirements.txt live_map/requirements.txt
RUN pip install --no-cache-dir -r live_map/requirements.txt

# No schemas/ needed here: AvroDeserializer fetches the schema each
# message was written with from Schema Registry itself.
COPY live_map/app.py live_map/
COPY live_map/static/ live_map/static/

EXPOSE 5000

# Lets `docker stop` shut the app down right away instead of force-killing
# it after 10s - see the same line in producer/Dockerfile.
STOPSIGNAL SIGINT

CMD ["python", "live_map/app.py"]
