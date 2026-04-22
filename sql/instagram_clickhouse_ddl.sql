CREATE DATABASE IF NOT EXISTS instagram_raw;
USE instagram_raw;

-- -----------------------------------------------------------------------------
-- Internal pipeline control/state tables
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

-- -----------------------------------------------------------------------------
-- Raw append-only tables (API payload normalized + payload_json)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_fb_pages
(
    page_id String,
    page_name Nullable(String),
    tasks Array(String),
    instagram_business_account_id Nullable(String),
    source_fetched_at DateTime64(3, 'UTC'),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (page_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_page_ig_binding
(
    page_id String,
    page_name Nullable(String),
    instagram_business_account_id Nullable(String),
    instagram_username Nullable(String),
    source_fetched_at DateTime64(3, 'UTC'),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (page_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_user_profile
(
    ig_user_id String,
    ig_id Nullable(String),
    username Nullable(String),
    name Nullable(String),
    biography Nullable(String),
    website Nullable(String),
    profile_picture_url Nullable(String),
    followers_count Nullable(UInt64),
    follows_count Nullable(UInt64),
    media_count Nullable(UInt64),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_media
(
    ig_user_id String,
    ig_media_id String,
    media_type LowCardinality(String),
    media_product_type Nullable(String),
    permalink Nullable(String),
    media_url Nullable(String),
    thumbnail_url Nullable(String),
    caption Nullable(String),
    username Nullable(String),
    is_comment_enabled Nullable(UInt8),
    like_count Nullable(UInt64),
    comments_count Nullable(UInt64),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, ig_media_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_media_children
(
    ig_user_id String,
    parent_media_id String,
    child_media_id String,
    media_type Nullable(String),
    media_product_type Nullable(String),
    permalink Nullable(String),
    media_url Nullable(String),
    thumbnail_url Nullable(String),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, parent_media_id, child_media_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_stories
(
    ig_user_id String,
    ig_story_id String,
    media_type Nullable(String),
    media_product_type Nullable(String),
    permalink Nullable(String),
    media_url Nullable(String),
    thumbnail_url Nullable(String),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, ig_story_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_media_insights
(
    ig_user_id String,
    ig_media_id String,
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
ORDER BY (ig_user_id, ig_media_id, metric, period, end_time, breakdown_key, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_user_insights
(
    ig_user_id String,
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
ORDER BY (ig_user_id, metric, period, end_time, breakdown_key, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_comments
(
    ig_user_id String,
    ig_media_id String,
    ig_comment_id String,
    parent_comment_id Nullable(String),
    text Nullable(String),
    username Nullable(String),
    like_count Nullable(UInt64),
    hidden Nullable(UInt8),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, ig_media_id, ig_comment_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_comment_replies
(
    ig_user_id String,
    ig_media_id String,
    parent_comment_id String,
    ig_reply_id String,
    text Nullable(String),
    username Nullable(String),
    like_count Nullable(UInt64),
    hidden Nullable(UInt8),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, ig_media_id, parent_comment_id, ig_reply_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_user_tags
(
    ig_user_id String,
    tagged_media_id String,
    media_type Nullable(String),
    permalink Nullable(String),
    caption Nullable(String),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, tagged_media_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_mentioned_media
(
    ig_user_id String,
    mentioned_media_id String,
    media_type Nullable(String),
    permalink Nullable(String),
    caption Nullable(String),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, mentioned_media_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_hashtag_lookup
(
    ig_user_id String,
    hashtag_name String,
    ig_hashtag_id String,
    source_fetched_at DateTime64(3, 'UTC'),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, hashtag_name, ig_hashtag_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_hashtag_top_media
(
    ig_user_id String,
    ig_hashtag_id String,
    ig_media_id String,
    media_type Nullable(String),
    permalink Nullable(String),
    caption Nullable(String),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, ig_hashtag_id, ig_media_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_hashtag_recent_media
(
    ig_user_id String,
    ig_hashtag_id String,
    ig_media_id String,
    media_type Nullable(String),
    permalink Nullable(String),
    caption Nullable(String),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, ig_hashtag_id, ig_media_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_business_discovery_profile
(
    source_ig_user_id String,
    discovered_ig_user_id String,
    discovered_username Nullable(String),
    discovered_name Nullable(String),
    biography Nullable(String),
    website Nullable(String),
    followers_count Nullable(UInt64),
    follows_count Nullable(UInt64),
    media_count Nullable(UInt64),
    source_fetched_at DateTime64(3, 'UTC'),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_ig_user_id, discovered_ig_user_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_business_discovery_media
(
    source_ig_user_id String,
    discovered_ig_user_id String,
    ig_media_id String,
    media_type Nullable(String),
    media_product_type Nullable(String),
    permalink Nullable(String),
    caption Nullable(String),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (source_ig_user_id, discovered_ig_user_id, ig_media_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_conversations
(
    ig_user_id String,
    conversation_id String,
    updated_time Nullable(DateTime64(3, 'UTC')),
    participants_json String DEFAULT '[]',
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, conversation_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_messages
(
    ig_user_id String,
    conversation_id String,
    message_id String,
    from_id Nullable(String),
    to_ids_json String DEFAULT '[]',
    text Nullable(String),
    created_time Nullable(DateTime64(3, 'UTC')),
    is_echo Nullable(UInt8),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, conversation_id, message_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_message_detail
(
    ig_user_id String,
    message_id String,
    conversation_id Nullable(String),
    created_time Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ingested_at)
ORDER BY (ig_user_id, message_id, ingested_at);

CREATE TABLE IF NOT EXISTS raw_ig_webhook_events
(
    event_id String,
    ig_user_id Nullable(String),
    object LowCardinality(String),
    event_field LowCardinality(String),
    source_event_time Nullable(DateTime64(3, 'UTC')),
    payload_json String,
    run_id String,
    received_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (object, event_field, event_id, received_at);

-- -----------------------------------------------------------------------------
-- Curated tables (deduplicated, query-friendly for Metabase)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS curated_ig_user_profile_current
(
    ig_user_id String,
    ig_id Nullable(String),
    username Nullable(String),
    name Nullable(String),
    biography Nullable(String),
    website Nullable(String),
    profile_picture_url Nullable(String),
    followers_count Nullable(UInt64),
    follows_count Nullable(UInt64),
    media_count Nullable(UInt64),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
ORDER BY ig_user_id;

CREATE TABLE IF NOT EXISTS curated_ig_media_current
(
    ig_user_id String,
    ig_media_id String,
    media_type LowCardinality(String),
    media_product_type Nullable(String),
    permalink Nullable(String),
    media_url Nullable(String),
    thumbnail_url Nullable(String),
    caption Nullable(String),
    username Nullable(String),
    is_comment_enabled Nullable(UInt8),
    like_count Nullable(UInt64),
    comments_count Nullable(UInt64),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
ORDER BY ig_media_id;

CREATE TABLE IF NOT EXISTS curated_ig_comments_current
(
    ig_comment_id String,
    ig_user_id String,
    ig_media_id String,
    parent_comment_id Nullable(String),
    text Nullable(String),
    username Nullable(String),
    like_count Nullable(UInt64),
    hidden Nullable(UInt8),
    source_timestamp Nullable(DateTime64(3, 'UTC')),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
ORDER BY ig_comment_id;

CREATE TABLE IF NOT EXISTS curated_ig_media_insights_timeseries
(
    ig_user_id String,
    ig_media_id String,
    metric LowCardinality(String),
    period LowCardinality(String),
    end_time Nullable(DateTime64(3, 'UTC')),
    breakdown_key Nullable(String),
    metric_value_float Nullable(Float64),
    metric_value_json String DEFAULT '{}',
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
PARTITION BY toYYYYMM(end_time)
ORDER BY (ig_media_id, metric, period, end_time, breakdown_key);

CREATE TABLE IF NOT EXISTS curated_ig_user_insights_timeseries
(
    ig_user_id String,
    metric LowCardinality(String),
    period LowCardinality(String),
    end_time Nullable(DateTime64(3, 'UTC')),
    breakdown_key Nullable(String),
    metric_value_float Nullable(Float64),
    metric_value_json String DEFAULT '{}',
    version_ts DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version_ts)
PARTITION BY toYYYYMM(end_time)
ORDER BY (ig_user_id, metric, period, end_time, breakdown_key);
