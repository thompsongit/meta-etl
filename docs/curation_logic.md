# Curation Logic and Data Integrity

## Design 

1. `raw_*` tables are source-near records with payload retention (`payload_json`) and ingestion metadata.
2. `curated_*` tables are query contracts for BI/Metabase with deterministic keys and `version_ts`.
3. Source-time columns (`source_timestamp`, `source_updated_at`, `source_event_time`, etc.) must represent source values only.
4. `ingested_at` and `version_ts` are ingestion-system times, not source event times.

## Curation Pattern

- Engine: `ReplacingMergeTree(version_ts)` for all curated tables.
- Keys: `ORDER BY` uses natural business keys per stream.
- Replacement model: latest `version_ts` wins for the same business key.

### Current-state curated tables

- Suffix: `_current`
- Grain: one logical latest row per business key.
- Examples:
  - `curated_ig_media_current`: key `(ig_user_id, ig_media_id)`
  - `curated_ig_comments_current`: key `(ig_user_id, ig_comment_id)`

### Timeseries curated tables

- Suffix: `_timeseries`
- Grain: one logical latest row per metric key.
- Example keys:
  - media insights: `(ig_user_id, ig_media_id, metric, period, end_time, breakdown_key)`
  - user insights: `(ig_user_id, metric, period, end_time, breakdown_key)`

## Integrity guarantees

1. Idempotent replay at logical level:
    - Reprocessing same source records may append duplicates in `raw_*`.
    - `curated_*` converges to latest logical row per key via replacing merge.

2. Windowed checkpoint safety:
    - Cursor/state advances only after successful window completion.
    - Failed window retries do not advance state.

3. Source-time correctness:
    - No fallback of source-time columns to ingestion time in transforms.
    - If source time is absent, source-time column remains `NULL`.

## Limits / Operational notes

1. `ReplacingMergeTree` deduplication is happens during merges. `FINAL` is used if immediate exact collapse is required for point checks.
2. `raw_*` is intentionally at-least-once and can contain duplicates due to retries.
3. Curated tables exist for all raw streams. Streams are populated by the sync pipeline.


