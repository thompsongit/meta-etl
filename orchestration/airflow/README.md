# Local Airflow Orchestration (IG ETL)

This folder runs a standalone Airflow installation that schedules the IG ETL container.

## What it runs
1. `postgres` (Airflow metadata DB)
2. `airflow-webserver`
3. `airflow-scheduler`
4. `airflow-triggerer`
5. `airflow-init` (one-off DB migrate + admin user create)

## Prerequisites
1. Docker is installed and running.
2. IG ETL container image exists locally:
```bash
docker build -t ig-etl:dev-local meta-etl
```
3. IG ETL env file exists (default expected path):
`meta-etl/scratch/.prod.env`

## First-time setup
1. Create local Airflow env:
```bash
cd meta-etl/orchestration/airflow
cp .env.example .env
```
Then set:
- `AIRFLOW_UID` to the user id (`id -u` on Linux)
- `IG_ETL_ENV_FILE_HOST_PATH` to env file path
- `DOCKER_GID` to the host docker group id (Linux): `getent group docker | cut -d: -f3`


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
1. Runs `ig-etl` container with:
```bash
python -m ig_etl --env-file /run/config/prod_env
```
2. Docker runtime options are controlled from `.env`:
- `IG_ETL_DOCKER_NETWORK_MODE` (`bridge` or `host`)
- `IG_ETL_EXTRA_HOST_MAPPING` (default `host.docker.internal:host-gateway`)
- `IG_ETL_CH_HOST`, `IG_ETL_CH_PORT`, `IG_ETL_CH_SECURE` (these are optional. they're overrides for ETL container)

## Local test flow
1. In the Airflow UI, unpause `ig_etl_daily`.
2. Trigger a run manually.
3. Verify task success in Airflow logs.
4. Verify run data rows appear in ClickHouse:
- `instagram_etl.etl_sync_runs`
- `instagram_etl.etl_stream_windows`


Tail scheduler logs:
```bash
docker compose logs -f airflow-scheduler
```

Restart stack:
```bash
docker compose down
docker compose up -d
```

If somehting breaks, reset deployment:
```bash
docker compose down -v
docker compose up airflow-init
docker compose up -d
```

If connection issues break the app:

1. Pick one connectivity mode and keep it consistent. This can fail things:

`bridge` mode:
```bash
IG_ETL_DOCKER_NETWORK_MODE=bridge
IG_ETL_EXTRA_HOST_MAPPING=host.docker.internal:host-gateway
IG_ETL_CH_HOST=host.docker.internal
IG_ETL_CH_PORT=8123
```

`host` mode:
```bash
IG_ETL_DOCKER_NETWORK_MODE=host
IG_ETL_CH_HOST=127.0.0.1
IG_ETL_CH_PORT=8123
```

2. Recreate scheduler/webserver/triggerer:
```bash
docker compose up -d --force-recreate airflow-scheduler airflow-webserver airflow-triggerer
```

3. Clear and rerun failed task in Airflow UI.

