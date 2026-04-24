# Local Airflow Orchestration (IG ETL)

This folder runs a standalone local Airflow stack that schedules the IG ETL container.

## What it runs
1. `postgres` (Airflow metadata DB)
2. `airflow-webserver`
3. `airflow-scheduler`
4. `airflow-triggerer`
5. `airflow-init` (one-off DB migrate + admin user create)

## Prerequisites
1. Docker is running.
2. IG ETL container image exists locally:
```bash
docker build -t ig-etl:dev-local /Users/mdot/Documents/dev/ig-etl
```
3. IG ETL env file exists (default expected path):
`/Users/mdot/Documents/dev/ig-etl/scratch/.prod.env`

## First-time setup
1. Create local Airflow env:
```bash
cd /Users/mdot/Documents/dev/ig-etl/orchestration/airflow
cp .env.example .env
```
Then set:
- `AIRFLOW_UID` to your local user id (`id -u` on macOS/Linux)
- `IG_ETL_ENV_FILE_HOST_PATH` to your real local env file path

Example:
```bash
sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
sed -i.bak "s|^IG_ETL_ENV_FILE_HOST_PATH=.*|IG_ETL_ENV_FILE_HOST_PATH=/Users/mdot/Documents/dev/ig-etl/scratch/.prod.env|" .env
```

2. Bring up stack:
```bash
docker compose up airflow-init
docker compose up -d
```

3. Open Airflow UI:
`http://localhost:8088`

Credentials are in `.env`:
- username: `AIRFLOW_WWW_USER_USERNAME`
- password: `AIRFLOW_WWW_USER_PASSWORD`

## DAG
`ig_etl_daily` in `dags/ig_etl_daily.py`

Behavior:
1. Precheck host env file path exists.
2. Runs `ig-etl` container with:
```bash
python -m ig_etl --env-file /run/config/.prod.env
```

## Local test flow
1. In UI, unpause `ig_etl_daily`.
2. Trigger manually.
3. Verify task success in Airflow logs.
4. Verify run rows in ClickHouse:
- `instagram_etl.etl_sync_runs`
- `instagram_etl.etl_stream_windows`

## Useful commands
View service status:
```bash
docker compose ps
```

Tail scheduler logs:
```bash
docker compose logs -f airflow-scheduler
```

Restart stack:
```bash
docker compose down
docker compose up -d
```

If permissions were previously broken, reset local stack state:
```bash
docker compose down -v
docker compose up airflow-init
docker compose up -d
```
