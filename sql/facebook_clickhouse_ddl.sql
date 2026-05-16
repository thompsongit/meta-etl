-- For dev where you may have a simpler ClickHouse locally:
CREATE DATABASE IF NOT EXISTS facebook_dev;

--CREATE DATABASE IF NOT EXISTS facebook_tel ON CLUSTER 'ja_analytics';
USE facebook_dev;

-- -----------------------------------------------------------------------------
-- Internal pipeline control/state tables (shared across ETLs)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_state
(
    account_id String,
    stream LowCardinality(String),
    cursor_value Nullable(String),
    cursor_ts Nullable(DateTime64(3, 'UTC')),
    lookback_hours UInt16 DEFAULT 72,
    last_successful_run_id Nullable(String),
    metadata_json String DEFAULT '{}',
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (account_id, stream);

CREATE TABLE IF NOT EXISTS etl_sync_runs
(
    run_id String,
    account_id String,
    stream LowCardinality(String),
    run_type LowCardinality(String), -- backfill | incremental
    status LowCardinality(String),   -- running | success | failed
    rows_extracted UInt64 DEFAULT 0,
    rows_loaded_raw UInt64 DEFAULT 0,
    rows_loaded_curated UInt64 DEFAULT 0,
    error_message Nullable(String),
    started_at DateTime64(3, 'UTC') DEFAULT now64(3),
    finished_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (account_id, stream, started_at, run_id);

CREATE TABLE IF NOT EXISTS etl_stream_windows
(
    account_id String,
    stream LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    window_id String,
    attempt UInt32,
    run_id String,
    status LowCardinality(String), -- running | success | failed | skipped
    rows_extracted UInt64 DEFAULT 0,
    rows_loaded_raw UInt64 DEFAULT 0,
    rows_loaded_curated UInt64 DEFAULT 0,
    error_message Nullable(String),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (account_id, stream, window_start, window_end);

CREATE TABLE IF NOT EXISTS etl_run_steps
(
    run_id String,
    account_id String,
    stream LowCardinality(String),
    window_id Nullable(String),
    step LowCardinality(String),
    status LowCardinality(String),
    message Nullable(String),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (account_id, run_id, created_at, step);

-- -----------------------------------------------------------------------------
-- Raw append-only tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_fb_page_profile
(
    fb_page_id String,
    name Nullable(String),
    username Nullable(String),
    category Nullable(String),
    about Nullable(String),
    description Nullable(String),
    link Nullable(String),
    fan_count Nullable(UInt64),
    followers_count Nullable(UInt64),
    verification_status Nullable(String),
    is_published Nullable(UInt8),
    overall_star_rating Nullable(Float64),
    rating_count Nullable(UInt64),
    --source_updated_at Nullable(DateTime64(3, 'UTC')),  breaks in FB v3.3
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (fb_page_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_fb_page_posts
(
    fb_page_id String,
    fb_post_id String,
    message Nullable(String),
    story Nullable(String),
    permalink_url Nullable(String),
    status_type Nullable(String),
    --post_type Nullable(String), -- this breaks. FB has deprected this in v3.3
    full_picture Nullable(String),
    shares_count Nullable(UInt64),
    reactions_count Nullable(UInt64),
    comments_count Nullable(UInt64),
    attachments_json String DEFAULT '{}',
    source_created_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (fb_page_id, fb_post_id, ingested_at);


CREATE TABLE IF NOT EXISTS raw_fb_post_comments
(
    fb_page_id String,
    fb_post_id String,
    fb_comment_id String,
    parent_comment_id Nullable(String),
    from_id Nullable(String),
    from_name Nullable(String),
    message Nullable(String),
    like_count Nullable(UInt64),
    comment_count Nullable(UInt64),
    is_hidden Nullable(UInt8),
    source_created_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (fb_page_id, fb_post_id, fb_comment_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_fb_page_insights
(
    fb_page_id String,
    entity_type LowCardinality(String),
    entity_id String,
    metric LowCardinality(String),
    period LowCardinality(String),
    end_time Nullable(DateTime64(3, 'UTC')),
    breakdown_key Nullable(String),
    metric_value_float Nullable(Float64),
    metric_value_json String DEFAULT '{}',
    title Nullable(String),
    description Nullable(String),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (
    fb_page_id,
    entity_type,
    entity_id,
    metric,
    period,
    ifNull(end_time, toDateTime64(0, 3, 'UTC')),
    ifNull(breakdown_key, ''),
    ingested_at
);

CREATE TABLE IF NOT EXISTS raw_fb_post_insights
(
    fb_page_id String,
    entity_type LowCardinality(String),
    entity_id String,
    metric LowCardinality(String),
    period LowCardinality(String),
    end_time Nullable(DateTime64(3, 'UTC')),
    breakdown_key Nullable(String),
    metric_value_float Nullable(Float64),
    metric_value_json String DEFAULT '{}',
    title Nullable(String),
    description Nullable(String),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (
    fb_page_id,
    entity_type,
    entity_id,
    metric,
    period,
    ifNull(end_time, toDateTime64(0, 3, 'UTC')),
    ifNull(breakdown_key, ''),
    ingested_at
);

-- -----------------------------------------------------------------------------
-- Curated tables
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated_fb_page_profile_current
(
    fb_page_id String,
    name Nullable(String),
    username Nullable(String),
    category Nullable(String),
    about Nullable(String),
    description Nullable(String),
    link Nullable(String),
    fan_count Nullable(UInt64),
    followers_count Nullable(UInt64),
    verification_status Nullable(String),
    is_published Nullable(UInt8),
    overall_star_rating Nullable(Float64),
    rating_count Nullable(UInt64),
    --source_updated_at Nullable(DateTime64(3, 'UTC')), --breaks in FB v3.3
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
ORDER BY fb_page_id;

CREATE TABLE IF NOT EXISTS curated_fb_page_posts_current
(
    fb_page_id String,
    fb_post_id String,
    message Nullable(String),
    story Nullable(String),
    permalink_url Nullable(String),
    status_type Nullable(String),
    --post_type Nullable(String), removed on v3.3 of FB API
    full_picture Nullable(String),
    shares_count Nullable(UInt64),
    reactions_count Nullable(UInt64),
    comments_count Nullable(UInt64),
    attachments_json String DEFAULT '{}',
    source_created_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
ORDER BY (fb_page_id, fb_post_id);

CREATE TABLE IF NOT EXISTS curated_fb_post_comments_current
(
    fb_page_id String,
    fb_post_id String,
    fb_comment_id String,
    parent_comment_id Nullable(String),
    from_id Nullable(String),
    from_name Nullable(String),
    message Nullable(String),
    like_count Nullable(UInt64),
    comment_count Nullable(UInt64),
    is_hidden Nullable(UInt8),
    source_created_at Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
ORDER BY (fb_page_id, fb_post_id, fb_comment_id);

CREATE TABLE IF NOT EXISTS curated_fb_page_insights
(
    fb_page_id String,
    entity_type LowCardinality(String),
    entity_id String,
    metric LowCardinality(String),
    period LowCardinality(String),
    end_time Nullable(DateTime64(3, 'UTC')),
    breakdown_key Nullable(String),
    metric_value_float Nullable(Float64),
    metric_value_json String DEFAULT '{}',
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
PARTITION BY toYYYYMM(ifNull(end_time, toDateTime64(0, 3, 'UTC')))
ORDER BY (
    fb_page_id,
    entity_type,
    entity_id,
    metric,
    period,
    ifNull(end_time, toDateTime64(0, 3, 'UTC')),
    ifNull(breakdown_key, '')
);

CREATE TABLE IF NOT EXISTS curated_fb_post_insights
(
    fb_page_id String,
    entity_type LowCardinality(String),
    entity_id String,
    metric LowCardinality(String),
    period LowCardinality(String),
    end_time Nullable(DateTime64(3, 'UTC')),
    breakdown_key Nullable(String),
    metric_value_float Nullable(Float64),
    metric_value_json String DEFAULT '{}',
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
PARTITION BY toYYYYMM(ifNull(end_time, toDateTime64(0, 3, 'UTC')))
ORDER BY (
    fb_page_id,
    entity_type,
    entity_id,
    metric,
    period,
    ifNull(end_time, toDateTime64(0, 3, 'UTC')),
    ifNull(breakdown_key, '')
);
