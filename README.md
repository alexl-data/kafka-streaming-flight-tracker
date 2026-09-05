# Live Flight Tracking – Streaming Data Pipeline with Kafka & Spark

Event-driven data engineering pipeline for live flight tracking: Kafka as
the central message broker, Spark Structured Streaming as the processing
layer, Schema Registry for Avro-based schema evolution, and ClickHouse as
the analytics sink.

Status: 🚧 in progress.

## Data Source

[OpenSky Network](https://opensky-network.org/) state vector API (REST,
free, no authentication required for basic usage). For a given bounding
box, it returns all currently received ADS-B position reports from
aircraft: position (lat/lon), altitude, speed, heading, callsign, country
of origin, transponder ID (`icao24`).

**Deliberate scope decisions:**
- Polling is limited to a bounded region (e.g. Europe/DACH) rather than the
  whole world – reduces data volume and respects the free API's rate
  limits.
- No matching against flight schedules/delays – that would require a
  second data source and would unnecessarily widen the scope. The focus is
  on flight density, altitude/speed distribution per region and time
  window.
- OpenSky is a **polling API**, not a true push stream (SSE/WebSocket).
  The producer polls the API every 5–10 seconds and translates each
  position report into an individual Kafka event. This is a deliberate
  trade-off against "true" streaming that occurs very frequently in
  practice whenever a source system cannot push.

## Architecture

```
OpenSky REST API (polling, 5-10s)
        │
        ▼
  Producer (Python)  ──serializes with──▶  Schema Registry (Avro)
        │
        ├──▶ Topic: flights.raw     (partitioned by region, event log)
        └──▶ Topic: flights.latest  (compacted, key = icao24, "current state")
        │
        ▼                                   │
                                             └──▶ DLQ topic (schema/parsing errors)
Spark Structured Streaming
  ├─ Avro deserialization (schema fetched from the registry via REST)
  ├─ Enrichment (region assignment, on-ground flag, etc.)
  ├─ Tumbling window aggregation + watermark (flight density/altitude
  │  distribution per region and time window, handling late events)
  └─ Checkpointing (exactly-once on the read side)
        │
        ▼
   Sink: ClickHouse (Gold: aggregations)  +  Parquet/Delta (Silver: enriched raw)
        │
        ▼
   Live map (small web app, consumes flights.latest / Gold aggregates)

Monitoring: Kafka UI (topics, schemas, consumer lag)
```

### Design Decisions

- **Bronze = the Kafka topic log.** Instead of a separate raw-data copy,
  the log itself (with sufficient retention) serves as the recovery point.
  Silver (parsed/enriched) and Gold (aggregations) are produced as Spark
  streaming outputs.
- **Two topics, two purposes.** `flights.raw` is a classic event log,
  partitioned by region, for parallel windowed aggregations.
  `flights.latest` is a **compacted topic** (key = `icao24`) that holds
  only the latest known position per aircraft – the basis for the live
  map, without having to scan the full event history.

## Tech Stack

- **Kafka** (KRaft mode, `confluentinc/cp-kafka`) – central message broker
- **Schema Registry** (Confluent, Avro) – schema evolution, compatibility checks
- **Kafka UI** – inspect topics, schemas, consumer lag visually
- **Spark Structured Streaming** (open-source PySpark, self-operated via
  Docker, no managed service) – compute layer for stream processing
- **ClickHouse** – sink for aggregations (Gold layer, serving for the live map)
- **GitHub Actions** – CI (tests, schema compatibility checks) and CD
  (Docker image build & push to GHCR)

### CI/CD Scope

Permanently deploying a Structured Streaming job to production would
require a 24/7 host (cluster/VM), which this portfolio project doesn't
have. Hence: **CD = automated image build & push** to GitHub Container
Registry on every merge to `main`, plus a documented (not continuously
operated) deploy step. This demonstrates CI/CD craftsmanship without
incurring permanent infrastructure cost for a demo.

## Roadmap / Commit Phases

- [x] Phase 0 – Repo scaffold, Kafka (KRaft) + Kafka UI via Docker Compose
- [ ] Phase 1 – Kafka fundamentals: simple JSON producer/consumer,
      consumer group demo (2 instances), explicit topic creation with
      partitioning
- [ ] Phase 2 – Schema Registry + Avro, first schema evolution
- [ ] Phase 3 – Error handling: dead-letter topic
- [ ] Phase 4 – Spark Structured Streaming: Kafka source, Avro
      deserialization, checkpointing
- [ ] Phase 5 – Windowing/watermarking: tumbling window aggregation,
      handling late events
- [ ] Phase 6 – Enrichment + sink (ClickHouse), idempotent writes
- [ ] Phase 7 – Monitoring: making consumer lag visible
- [ ] Phase 8 – CI: lint/tests, schema compatibility check, Docker Compose validation
- [ ] Phase 9 – CD: image build & push to GHCR
- [ ] Phase 10 – Live map, README/architecture docs, polish

## Setup (as of Phase 0)

```bash
docker compose up -d
```

- Kafka broker: `localhost:9092`
- Kafka UI: [http://localhost:8081](http://localhost:8081)
