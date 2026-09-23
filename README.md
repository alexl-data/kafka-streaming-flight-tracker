# Live Flight Tracking – Streaming Data Pipeline with Kafka & Spark

Event-driven data engineering pipeline for live flight tracking: Kafka as
the central message broker, Spark Structured Streaming as the processing
layer, Schema Registry for Avro-based schema evolution, and ClickHouse as
the analytics sink.

## Data Source

[OpenSky Network](https://opensky-network.org/) state vector API (REST,
free, no authentication required for basic usage). For a given bounding
box, it returns all currently received ADS-B position reports from
aircraft: position (lat/lon), altitude, speed, heading, callsign, country
of origin, transponder ID (`icao24`).

**Deliberate scope decisions:**
- Polling is limited to a bounded region (DACH: Germany, Austria,
  Switzerland) rather than the whole world – reduces data volume and
  respects the free API's rate limits.
- OpenSky is a polling API, not a true push stream.
  The producer polls the API every 15 seconds and translates each
  position report into an individual Kafka event. This is a deliberate
  trade-off against "true" streaming that occurs very frequently in
  practice whenever a source system cannot push.

## Architecture

```
OpenSky REST API (polling, 15s)
        │
        ▼
  Producer (Python)  ──serializes with──▶  Schema Registry (Avro)
        │
        ├──▶ Topic: flights.raw     (partitioned by region, event log)
        ├──▶ Topic: flights.latest  (compacted, key = icao24, "current state")
        └──▶ Topic: flights.raw.dlq (serialization errors)
                                                  ▲
Spark Structured Streaming                        │
  ├─ Magic-byte check on flights.raw ──(malformed)┘
  ├─ Avro deserialization (schema fetched from the registry via REST)
  ├─ Stream-static join against emergency squawk codes
  ├─ Tumbling window aggregation + watermark (flight count/altitude/speed/
  │  emergency count per region and time window, handling late events)
  └─ Checkpointing (exactly-once on the read side)
        │
        ▼
   Sink: ClickHouse (Gold: region_traffic, idempotent via ReplacingMergeTree)
        │
        ▼
   Live map (small web app, consumes flights.latest / Gold aggregates)

Monitoring: Kafka UI (topics, schemas) + Spark StreamingQueryListener
  (query progress — Spark's Kafka source doesn't commit offsets, so it
  never shows up in classic consumer-lag tooling)
```

- **Bronze = the Kafka topic log.** Instead of a separate raw-data copy,
  the log itself (with sufficient retention) serves as the recovery point.
  Silver (parsed/enriched) exists only as an in-flight Spark
  transformation, not a separately persisted layer; Gold (aggregations)
  is what actually gets persisted, to ClickHouse.
- **Two topics, two purposes.** `flights.raw` is a classic event log,
  partitioned by region, for parallel windowed aggregations.
  `flights.latest` is a **compacted topic** (key = `icao24`) that holds
  only the latest known position per aircraft – the basis for the live
  map, without having to scan the full event history.

## Tech Stack

- **Kafka** (KRaft mode, `confluentinc/cp-kafka`) – central message broker
- **Schema Registry** (Confluent, Avro) – schema evolution, compatibility checks
- **Kafka UI** – inspect topics and schemas visually
- **Spark Structured Streaming** (open-source PySpark, run locally via
  `spark-submit`, no managed service) – compute layer for stream processing
- **ClickHouse** – sink for aggregations (Gold layer, serving for the live map)
- **GitHub Actions** – CI (tests, schema compatibility checks) and CD
  (Docker image build & push to GHCR)

## CI/CD Scope

Permanently deploying a Structured Streaming job to production would
require a 24/7 host (cluster/VM), which this portfolio project doesn't
have. Hence: **CD = automated image build & push** to GitHub Container
Registry on every merge to `main`, plus a documented (not continuously
operated) deploy step. This demonstrates CI/CD craftsmanship without
incurring permanent infrastructure cost for a demo.

## Cost / Latency Trade-offs

Several settings already introduced above are, at their core, the same
kind of decision - trading a bit of latency or completeness for lower
cost or simpler infrastructure, or the reverse:

- **OpenSky polling interval (15s):** bounded by the anonymous API's free
  daily quota (400 requests/day), not chosen as a latency target - data is
  inherently up to 15s stale before it's even fetched.
- **Spark trigger interval (matched to the same 15s):** pure overhead
  reduction, no latency trade-off - checking Kafka faster than the
  producer can supply data would not make results arrive any sooner.
- **1-minute windows + 1-minute watermark:** a direct completeness-vs-latency
  trade-off - a longer watermark would catch more genuinely late events
  but delays when a window counts as final.
- **No 24/7 deployment target (see CI/CD Scope above):** full production
  realism traded for zero ongoing infrastructure cost.
- **ClickHouse capped at 2 GB:** a small, fixed ceiling appropriate for
  the data volume this project actually produces, not provisioned for
  scale it doesn't need.

## Log Compaction

Every record the producer sends to `flights.raw` is also sent to
`flights.latest`, same schema, keyed by `icao24` instead of `region`.
Created with `cleanup.policy=compact`: Kafka retains only the most recent
message per key there, so the topic holds "current position of every
aircraft" without growing unbounded - unlike `flights.raw`'s append-only
log, which keeps every event. One `AvroSerializer` instance handles both
topics; confluent-kafka derives the Schema Registry subject from the topic
given to each individual `produce()` call, so this needed no second
serializer, just a second `produce()` call with the same record.

For local development the topic also sets aggressive compaction settings
(`segment.ms=60000`, `min.cleanable.dirty.ratio=0.01`,
`delete.retention.ms=100`) - production defaults compact far less
eagerly, which would make this untestable within a normal working
session.

Verified directly, not just configured: compaction only ever touches
*closed* segments, never the currently active one, so a key's older
records survive until its segment closes - eventual, not immediate,
consistency, the same category of behavior as ClickHouse's
`ReplacingMergeTree` (see ClickHouse Sink) but through a different
mechanism.

## Spark Structured Streaming

`spark/flights_stream.py` reads `flights.raw`, deserializes the Avro
payload, left-joins each event against a small static table of emergency
squawk codes, and aggregates flight count, average altitude, average
speed, and emergency-squawk count per region in 1-minute tumbling
windows, written to ClickHouse (see ClickHouse Sink below). Must run via
`spark-submit` (not plain `python`) so the Kafka/Avro connector packages
resolve onto the classpath:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.spark:spark-avro_2.13:4.2.0 spark/flights_stream.py
```

Things worth calling out:

- **No Schema Registry integration in open-source Spark.** Plain Spark's `from_avro()` doesn't know how to talk
  to a Schema Registry or parse Confluent's wire format. The job fetches
  the current schema once via REST at startup and manually strips the
  5-byte magic-byte-plus-schema-ID header from each message before
  `from_avro()` can parse it. Reads `startingOffsets: latest` rather than
  `earliest`, since `from_avro()`'s single fixed schema can't decode
  `flights.raw`'s older schema records (see Schema Evolution below).
- **Event time, not processing time.** Windows are keyed on
  `time_position` (when OpenSky recorded the position), not the wall-clock
  time Spark processes each micro-batch - the two diverge under load or a
  slow producer, which is exactly what watermarking needs to reason about.
- **Stream-static join.** The emergency-squawk lookup is a plain
  `spark.createDataFrame()`, not read from Kafka, so joining it against
  the streaming DataFrame is an ordinary join rather than a stream-stream
  join (which Structured Streaming restricts more heavily).

## ClickHouse Sink

No Spark Structured Streaming connector exists for ClickHouse, so writes
go through `foreachBatch` - Spark hands each micro-batch to a plain Python
function, which inserts it via the `clickhouse-connect` client. Target
table `region_traffic` (`clickhouse/init.sql`) uses the `ReplacingMergeTree`
engine, ordered by `(window_start, region)`, versioned by an `updated_at`
column - the mechanism that makes writing the same window idempotent.

`outputMode("update")` means a still-open window gets re-inserted on every
batch as its count grows, so the raw table temporarily holds multiple
versions of the same `(window_start, region)` - visible directly with a
plain `SELECT`. Querying with `SELECT ... FROM region_traffic FINAL`
collapses that down to one row per key, keeping only the highest
`updated_at`.

## Schema Evolution

Schema Registry's default compatibility mode is `BACKWARD`: a new schema
must be able to read data written with the previous schema. Verified two
ways:

- **Compatible change:** added `ingested_at` (nullable, `default: null`) to
  `flight_state.avsc`. Registered automatically as a new schema version on
  the next producer run; a single consumer using one `AvroDeserializer`
  correctly reads records written both before and after the change from
  `flights.raw`.
- **Incompatible change:** registering a schema with a new *required*
  field (no default) is rejected outright (`HTTP 409`,
  `READER_FIELD_MISSING_DEFAULT_VALUE`) - nothing gets written. This is
  the same check Phase 8's CI pipeline will run automatically before a
  merge.

Note: the Schema Registry rejection above only fires when a schema is
*registered*. If producer code adds a dict key that was never added to the
`.avsc` file, Avro serialization doesn't error - it silently drops any
field not defined in the schema. The schema controls what actually gets
written, not the Python dict.

## Error Handling

A single malformed record shouldn't crash the producer or the Spark job
that's otherwise processing thousands of good ones. Both route failures
to `flights.raw.dlq` (1 partition - DLQ volume is expected to stay low,
unlike `flights.raw`'s partitioning-by-key-cardinality reasoning) instead
of stopping or silently dropping the message.

Both failure paths write the **same** `DlqEnvelope` schema
(`schemas/dlq_envelope.avsc`), registered in Schema Registry like any
other topic - one shared format instead of each side writing its own
shape, which would leave `flights.raw.dlq` mixing incompatible payloads
in one topic. The envelope keeps the original failed payload as a
`bytes` field and standardizes only the metadata around it
(`stage`, `error`, `error_type`, `source_topic`/`partition`/`offset`,
`failed_at`):

- **Producer:** if a record is missing a required field (e.g. an
  incomplete OpenSky response), Avro serialization raises
  `ValueSerializationError` synchronously inside `produce()`. Caught
  per-record; the record's JSON form (it never became valid Avro) is
  wrapped in the envelope with `stage: "producer"` and no
  partition/offset (it never reached one). 
- **Spark:** `from_avro()` has no error handling for malformed input at
  all - its `mode: "PERMISSIVE"` option silently decodes invalid bytes
  into a plausible-looking wrong value instead of failing (Avro's binary
  format has no checksum to validate against). `spark/flights_stream.py`
  instead checks the Confluent wire-format magic byte itself before
  `from_avro()` runs, filtering each micro-batch row by row into a valid
  DataFrame (parsed and windowed as normal) and an invalid one (wrapped
  in the same envelope with `stage: "spark"`, written to the DLQ via a
  second concurrent `foreachBatch` query) - individual records within the
  same batch can end up on either side. This mirrors a check confluent-kafka's
  Python `AvroDeserializer` library already does internally on the read
  side - Spark's `from_avro()` has no equivalent built in, so the check
  has to be made explicit here.

A single DLQ-reading consumer can therefore deserialize every entry the
same way regardless of which stage produced it.

## Monitoring

`spark/flights_stream.py` is invisible to classic Kafka consumer-lag
tooling (Kafka UI, `kafka-consumer-groups --describe`) - Spark's Kafka
source never commits offsets to Kafka's consumer group coordinator, and
doesn't show up in `kafka-consumer-groups --list` at all. The
`kafka.group.id` option looks like it should fix this, but Spark's own
docs scope it to authorization scenarios and warn against setting it
otherwise - and it doesn't make the job visible either way. There's no
supported way to make a Structured Streaming Kafka source visible to
classic consumer-lag tooling - its progress is tracked instead through
Spark's own `StreamingQueryListener` API
(`ProgressLogger` in `flights_stream.py`), which reports per-batch
throughput and duration for both queries directly from Spark. Both
queries also run on a `Trigger.ProcessingTime` matched to the producer's
15-second poll interval, rather than Spark's default of checking for new
data continuously - polling faster than the producer can supply it only
adds empty-microbatch overhead.

## Setup

```bash
docker compose up -d
python -m venv .venv
.venv\Scripts\activate       # Windows
pip install -r requirements.txt
docker exec -i clickhouse clickhouse-client --multiquery < clickhouse/init.sql
```

Topic auto-creation is disabled (see `docker-compose.yml`), so the topics
need to be created explicitly once, after the containers are up:

```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic flights.raw --partitions 4 --replication-factor 1

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic flights.latest --partitions 4 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.ms=60000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=100

docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic flights.raw.dlq --partitions 1 --replication-factor 1
```

- Kafka broker: `localhost:9092`
- Kafka UI: [http://localhost:8081](http://localhost:8081)
- Schema Registry: [http://localhost:8082](http://localhost:8082)
- ClickHouse: [http://localhost:8123](http://localhost:8123) (HTTP), `localhost:9000` (native)

Then, in separate terminals, start the producer and the Spark job:

```bash
python producer/flights_producer.py
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.spark:spark-avro_2.13:4.2.0 spark/flights_stream.py
```

### Additional Prerequisites for Spark

PySpark needs a JVM - not installed by `pip`:

- **Java 17** (e.g. [Eclipse Temurin](https://adoptium.net/)), with `JAVA_HOME` pointing at the install directory.
- **Windows only:** Spark's local filesystem operations go through Hadoop's
  `FileUtil`, which needs `winutils.exe` even for a purely local job with no
  HDFS involved - without it, Spark fails at startup with
  `HADOOP_HOME and hadoop.home.dir are unset`. Download `winutils.exe` and
  `hadoop.dll` for a Hadoop 3.3.x build (e.g. from
  [cdarlint/winutils](https://github.com/cdarlint/winutils)) into a
  `bin/` folder, and set `HADOOP_HOME` to its parent directory.
- **Windows only:** `spark-submit.cmd` resolves its own install location
  (`SPARK_HOME`) via an internal `python` call before it does anything
  else - and that call ignores the active venv, hitting Windows' "Python
  was not found" App Execution Alias stub instead, which then surfaces as
  `Failed to find Spark jars directory`. Set `SPARK_HOME` to the
  `pyspark` package's install directory (e.g.
  `<venv>\Lib\site-packages\pyspark`) to skip that lookup entirely. For
  the same underlying reason, also set `PYSPARK_PYTHON` and
  `PYSPARK_DRIVER_PYTHON` to the venv's `python.exe` so the job itself
  doesn't hit the same stub once it's actually running.

## Roadmap / Commit Phases

- [x] Phase 0 – Repo scaffold, Kafka (KRaft) + Kafka UI via Docker Compose
- [x] Phase 1 – Kafka fundamentals: explicit topic creation with
      partitioning against the real `flights.raw` topic (an earlier
      version of this phase used standalone demo scripts and a separate
      `demo.events` topic to also show consumer-group rebalancing - later
      removed to keep the repo scoped to the real pipeline only; see git
      history)
- [x] Phase 2a – Live data source: OpenSky Network producer (DACH bounding box)
- [x] Phase 2b – Schema Registry + Avro serialization for the real pipeline
- [x] Phase 2c – First schema evolution
- [x] Phase 3 – Error handling: dead-letter topic
- [x] Phase 4 – Spark Structured Streaming: Kafka source, Avro
      deserialization, checkpointing
- [x] Phase 5 – Windowing/watermarking: tumbling window aggregation,
      handling late events
- [x] Phase 6 – Enrichment + sink (ClickHouse), idempotent writes
- [x] Phase 7 – Monitoring: pipeline progress visible via Spark's own
      streaming query metrics (`StreamingQueryListener`)
- [ ] Phase 8 – CI: lint/tests, schema compatibility check, Docker Compose validation
- [ ] Phase 9 – CD: image build & push to GHCR
- [ ] Phase 10 – Live map, README/architecture docs, polish
