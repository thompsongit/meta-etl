## Instagram -> ClickHouse ETL/CDC

### Project layout
- `sql/instagram_clickhouse_ddl.sql`: ClickHouse schema (`raw_*`, `curated_*`, `etl_state`, `etl_sync_runs`)
- `src/ig_etl_sync.py`: CLI entrypoint for sync runs
- `src/ig_etl/config.py`: runtime config/arg parsing
- `src/ig_etl/pipeline.py`: orchestration for profile/media/insights/comments/extended streams/state
- `src/ig_etl/graph_api.py`: Graph API client + retries/pagination
- `src/ig_etl/clickhouse_store.py`: ClickHouse read/write helpers
- `src/ig_etl/transform.py`: payload-to-row mapping

### 1) Use the project pyenv
`.python-version` should point to `3.12.11` and checks should run in that env.

Create a pyenv venv named `ig-etl` from Python `3.12.11` and proceed as  follows:
```bash
pyenv local ig-etl
pyenv shell ig-etl
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### Container image (Airflow runtime contract)
Build locally:
```bash
docker build -t ig-etl:dev-local .
```

Container CLI smoke test:
```bash
docker run --rm ig-etl:dev-local --help
```

Run sync via container:
```bash
docker run --rm --env-file .prod.env ig-etl:dev-local
```

If ClickHouse is running on host machine, set `CH_HOST=host.docker.internal` in the env file for container runs.

For ClickHouse clusters:
- Set `CH_CLUSTER=<cluster_name>` (validated on startup via `system.clusters`)
- Optional failover endpoints: `CH_ALT_HOSTS=ch-node-2:8123,ch-node-3:8123`
- Ensure schema exists on every failover node. The provided `sql/instagram_clickhouse_ddl.sql` is local-table DDL (no `ON CLUSTER`/`Replicated*` engines).

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
- for comments: `instagram_business_manage_comments`

### 5) Run sync
Auto-loads env from `.prod.env` 

Run directly:
```bash
python src/ig_etl_sync.py
```

Or force a specific env file:
```bash
python src/ig_etl_sync.py --env-file /absolute/path/to/.env
```

### Sync behavior
- Pulls profile snapshot
- Plans bounded sync windows and writes per-window state/log rows
- INITIAL SYNC: Starts from `INITIAL_SYNC_START_AT` (or `BACKFILL_DAYS` fallback) and processes `BACKFILL_CHUNK_DAYS` windows to now
- INCREMENTAL SYNC: runs from cursor-minus-lookback to now
- Pulls user insights and media insights
- Pulls comments incrementally across recent media 
- Pulls extended streams (children, stories, tags, mentioned media, replies, hashtags, business discovery, conversations/messages) unless disabled
- Loads both `raw_*` and `curated_*` tables into ClickHouse
- Commits `ig_media` and `ig_comments` state only after successful window writes
- Logs run-level status in `etl_sync_runs`, chunk/window status in `etl_stream_windows`, and details in `etl_run_steps`

### Useful flags (might be useful someday)
- `--disable-comments`: run media + insights only
- `--disable-extended-streams`: disable optional extended endpoint extraction
- `--initial-sync-start-at 2024-01-01T00:00:00Z`: bootstrap epoch start
- `--backfill-chunk-days 60`: bootstrap/catchup window size
- `--max-windows-per-run 0`: cap windows per run (0 = no cap)
- `--hashtag-names "fitness,wellness"`: hashtag lookup/top/recent media probes
- `--business-discovery-usernames "competitor_a,competitor_b"`: business discovery profile/media probes
- `--messages-page-size 100`: page size for conversations/messages probes
- `--comments-page-size 50`: comments page size per media request (API max 50)
- `--comments-media-scan-limit 200`: max media to probe for comments each run
- `--comments-lookback-hours 72`: lookback for comments incremental window
- `--comments-backfill-days 90`: first-run backfill window for comments
- `--lock-file /tmp/ig_etl_sync.lock`: prevents overlapping runs


### CI image publishing
Workflow:
- `.github/workflows/ig-etl-image.yml`

### Airflow orchestration
Airflow is preferred for scheduling runs. Files at:
- `orchestration/airflow/docker-compose.yml`
- `orchestration/airflow/dags/ig_etl_daily.py`
- `orchestration/airflow/README.md`
