-- Gold-layer table: one row per (time window, region), holding the
-- windowed aggregation Spark computes in spark/flights_stream.py.
--
-- ReplacingMergeTree keeps only the row with the highest `updated_at` for
-- a given ORDER BY key (window_start, region) once ClickHouse merges
-- parts in the background (or when queried with FINAL) - this is what
-- makes re-writing the same window idempotent: Spark can safely insert
-- the same (window_start, region) again after a retry/replay, and it
-- collapses down to a single current row instead of duplicating.
CREATE TABLE IF NOT EXISTS region_traffic
(
    window_start DateTime,
    window_end DateTime,
    region String,
    flight_count UInt32,
    -- Nullable: a window can be all grounded aircraft, which report no
    -- altitude/velocity, so AVG() over the group is legitimately NULL,
    -- not missing/bad data.
    avg_altitude_m Nullable(Float64),
    avg_velocity_kmh Nullable(Float64),
    -- Set from Spark's current_timestamp() (see flights_stream.py), which
    -- Spark fixes once per micro-batch at query analysis time - the same
    -- value for every row in a batch, but a fresh, later value on every
    -- new batch (replays included, since it reflects real wall-clock
    -- processing time). Never derive this from event data such as
    -- window_start: two writes of the same window would then tie on
    -- version, and ReplacingMergeTree doesn't define a winner for ties.
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, region);
