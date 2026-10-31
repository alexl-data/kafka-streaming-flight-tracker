# Built from the repo root (see docker-compose.yml) - same reason as
# producer/Dockerfile. Pinned to bookworm: Debian 13 (the newer default
# python:3.13-slim base) no longer packages Java 17.
FROM python:3.13-slim-bookworm

# PySpark is only the Python API - the Spark engine itself runs on the JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    SPARK_PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.spark:spark-avro_2.13:4.2.0

WORKDIR /app

COPY spark/requirements.txt spark/requirements.txt
RUN pip install --no-cache-dir -r spark/requirements.txt

# Spark needs two add-on packages to read from Kafka and decode Avro (the
# SPARK_PACKAGES above). spark-submit --packages downloads them on startup -
# in a container that would happen on every start, since each container
# begins with the image's files only. Starting Spark once here at build
# time downloads them into the image instead, so later starts reuse them.
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.config('spark.jars.packages', '$SPARK_PACKAGES').getOrCreate().stop()"

COPY schemas/ schemas/
COPY spark/flights_stream.py spark/

# 4040 = Spark UI (jobs, stages, streaming query statistics).
EXPOSE 4040

# sh is only needed to fill in $SPARK_PACKAGES. exec then replaces sh with
# spark-submit, so `docker stop`'s shutdown request reaches Spark itself -
# otherwise sh would receive it without passing it on, and Docker would
# force-kill Spark after a 10-second timeout instead of a clean shutdown.
CMD ["sh", "-c", "exec spark-submit --packages \"$SPARK_PACKAGES\" spark/flights_stream.py"]
