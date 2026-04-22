## Instagram -> ClickHouse ETL/CDC

### Project layout
- `sql/instagram_clickhouse_ddl.sql`: ClickHouse schema (`raw_*`, `curated_*`, `etl_state`, `etl_sync_runs`)
- `src/ig_etl_sync.py`: CLI entrypoint for sync runs
- `src/ig_etl/config.py`: runtime config/arg parsing
- `src/ig_etl/pipeline.py`: orchestration for profile/media/insights/comments/state
- `src/ig_etl/graph_api.py`: Graph API client + retries/pagination
- `src/ig_etl/clickhouse_store.py`: ClickHouse read/write helpers
- `src/ig_etl/transform.py`: payload-to-row mapping

### 1) Use the project pyenv
`.python-version` should point to `ig-etl` and checks should run in that env.

```bash
pyenv local ig-etl
pyenv shell ig-etl
python --version
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Create tables
```bash
clickhouse-client --queries-file sql/instagram_clickhouse_ddl.sql
```

### 4) Validate token and permissions
```bash
IG_GRAPH_TOKEN='...' python scratch/ig_auth_smoketest.py
```

For direct Instagram Login mode (`graph.instagram.com`), expected ETL scopes are:
- `instagram_business_basic`
- `instagram_business_manage_insights`
- optional for comments: `instagram_business_manage_comments`

### 5) Run sync
Auto-loads env from `.prod.env` 

Run directly:
```bash
python src/ig_etl_sync.py
```

Or force a specific env file:
```bash
python src/ig_etl_sync.py --env-file /absolute/path/to/.prod.env
```

### Sync behavior
- Pulls profile snapshot
- Incrementally pulls media using `etl_state` cursor + lookback window
- Pulls user insights and media insights
- Pulls comments incrementally across recent media (skip with `--disable-comments`)
- Loads both `raw_*` and `curated_*` tables
- Commits `ig_media` and `ig_comments` state only after successful writes

### Useful flags
- `--disable-comments`: run media + insights only
- `--comments-page-size 50`: comments page size per media request (API max 50)
- `--comments-media-scan-limit 200`: max media to probe for comments each run
- `--comments-lookback-hours 72`: lookback for comments incremental window
- `--comments-backfill-days 90`: first-run backfill window for comments
- `--lock-file /tmp/ig_etl_sync.lock`: prevents overlapping runs

The limits help comply with rate limits of the IG graph API
