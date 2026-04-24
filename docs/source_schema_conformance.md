# Instagram Source Schema Conformance Notes


## Extractor Coverage 

The sync implementation in `src/ig_etl/pipeline.py` writes raw + curated rows for all streams declared in DDL:

1. `GET /{ig_user_id}` -> profile
2. `GET /{ig_user_id}/media`
3. `GET /{ig_media_id}/children`
4. `GET /{ig_user_id}/stories`
5. `GET /{ig_user_id}/insights`
6. `GET /{ig_media_id}/insights`
7. `GET /{ig_media_id}/comments`
8. `GET /{ig_comment_id}/replies`
9. `GET /{ig_user_id}/tags`
10. `GET /{ig_user_id}/mentioned_media`
11. `GET /ig_hashtag_search`
12. `GET /{ig_hashtag_id}/top_media`
13. `GET /{ig_hashtag_id}/recent_media`
14. `GET /{ig_user_id}?fields=business_discovery.username(...)`
15. `GET /{ig_user_id}/conversations`
16. `GET /{conversation_id}/messages`
17. `GET /{message_id}`


## Timestamp Semantics

1. Source-time columns are source-derived only:
    - `source_timestamp`, `source_updated_at`, `end_time`, `source_event_time` remain `NULL` when source omits the value.
2. Ingestion-time columns are pipeline-derived:
    - `ingested_at` and curated `version_ts` track load/versioning time.


## And...

1. Raw tables are source-near but normalized; each row still retains full sampled payload in `payload_json`.
2. User/media insights are flattened from the API  into metric rows by `(metric, period, end_time, breakdown_key)`.
3. Some endpoints may be unavailable for specific app mode/scopes/account types (and not sure when these change). The extractor logs warning and skips those streams without failing the full run unless the error is fatal.



