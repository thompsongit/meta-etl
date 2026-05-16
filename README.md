## Instagram/Facebook -> ClickHouse ETL/CDC

### Project layout
- `sql/instagram_clickhouse_ddl.sql`: ClickHouse schema (`raw_*`, `curated_*`, `etl_state`, `etl_sync_runs`)
- `sql/facebook_clickhouse_ddl.sql`: Facebook ClickHouse schema (`raw_fb_*`, `curated_fb_*`, shared control tables)
- `src/ig_etl_sync.py`: CLI entrypoint for sync runs
- `src/fb_etl_sync.py`: Facebook CLI entrypoint for sync runs
- `src/ig_etl/config.py`: runtime config/arg parsing
- `src/ig_etl/pipeline.py`: orchestration for profile/media/insights/comments/extended streams/state
- `src/ig_etl/graph_api.py`: Graph API client + retries/pagination
- `src/ig_etl/clickhouse_store.py`: ClickHouse read/write helpers
- `src/ig_etl/transform.py`: payload-to-row mapping
- `src/fb_etl/*`: Facebook implementation mirroring the IG implementation (page profile, posts/feed, comments(not working), page insights, post insights)

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
docker build -t fb-etl:dev-local -f docker/Dockerfile.fb .
```

Container CLI smoke test:
```bash
docker run --rm ig-etl:dev-local --help
docker run --rm fb-etl:dev-local --help
```

Run sync via container:
```bash
docker run --rm --env-file .prod.env ig-etl:dev-local
docker run --rm --env-file .prod.fb.env fb-etl:dev-local
```

If ClickHouse is running on host machine, set `CH_HOST=host.docker.internal` in the env file for container runs.

For ClickHouse clusters:
- Set `CH_CLUSTER=<cluster_name>` (validated on startup via `system.clusters`)
- Optional failover endpoints: `CH_ALT_HOSTS=ch-node-2:8123,ch-node-3:8123`
- Ensure schema exists on all ClickHouse nodes

### 3) Create tables
```bash
clickhouse-client --queries-file sql/instagram_clickhouse_ddl.sql
clickhouse-client --queries-file sql/facebook_clickhouse_ddl.sql
```

### 4) Validate token and permissions
```bash
IG_GRAPH_TOKEN='...' python scratch/ig_auth_smoketest.py
FB_GRAPH_TOKEN='...' FB_PAGE_ID='...' python scratch/fb_auth_smoketest.py
```

For direct Instagram Login mode (`graph.instagram.com`), expected ETL scopes are:
- `instagram_business_basic`
- `instagram_business_manage_insights`
- for comments: `instagram_business_manage_comments`

### 5) Run sync
IG auto-loads env from `.prod.env`. FB auto-loads from `.prod.fb.env` (then `.prod.env` fallback). 

Run directly:
```bash
python src/ig_etl_sync.py
python src/fb_etl_sync.py
```

Or force a specific env file:
```bash
python src/ig_etl_sync.py --env-file /absolute/path/to/.env
python src/fb_etl_sync.py --env-file /absolute/path/to/.env
```

### Sync behavior
- Both ETLs use bootstrap/catchup/incremental windows with stateful cursors and lookback overlap for retry-safe CDC behavior.
- IG pulls profile/media/insights/comments + optional extended IG streams.
- FB pulls page profile/posts/page insights/post insights. Comments not on this API version 25.0 with graph v3.3
- Both load `raw_*` + `curated_*`, commit state only after successful window completion, and log runs/windows/steps in control tables.

### Useful flags (might be useful someday)
IG examples:
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

FB env template:
- `prod.fb.env.example` (copy and edit to `.prod.fb.env`)


### CI image publishing
Workflow:
- `.github/workflows/ig-etl-image.yml`
- `.github/workflows/fb-etl-image.yml`

### Airflow orchestration
Airflow is preferred for scheduling runs. Files at:
- `orchestration/airflow/docker-compose.yml`
- `orchestration/airflow/dags/ig_etl_daily.py`
- `orchestration/airflow/dags/fb_etl_daily.py`
- `orchestration/airflow/README.md`
