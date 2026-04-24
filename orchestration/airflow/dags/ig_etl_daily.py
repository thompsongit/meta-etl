from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


IG_ETL_IMAGE = os.getenv("IG_ETL_IMAGE", "ig-etl:dev-local")
IG_ETL_ENV_FILE_HOST_PATH = os.getenv(
    "IG_ETL_ENV_FILE_HOST_PATH",
    "/home/dev/ig-etl/scratch/.prod.env",
)
IG_ETL_DAG_CRON = os.getenv("IG_ETL_DAG_CRON", "0 4 * * *")
IG_ETL_DOCKER_NETWORK_MODE = os.getenv("IG_ETL_DOCKER_NETWORK_MODE", "bridge")
IG_ETL_CH_HOST = os.getenv("IG_ETL_CH_HOST", "").strip()
IG_ETL_CH_PORT = os.getenv("IG_ETL_CH_PORT", "").strip()
IG_ETL_CH_SECURE = os.getenv("IG_ETL_CH_SECURE", "").strip()

_extra_hosts: dict[str, str] = {}
extra_host_mapping = os.getenv("IG_ETL_EXTRA_HOST_MAPPING", "host.docker.internal:host-gateway")
if extra_host_mapping and ":" in extra_host_mapping:
    host, target = extra_host_mapping.split(":", 1)
    host = host.strip()
    target = target.strip()
    if host and target:
        _extra_hosts[host] = target

CONTAINER_ENV_FILE_PATH = "/run/config/prod_env"
_container_env: dict[str, str] = {}
if IG_ETL_CH_HOST:
    _container_env["CH_HOST"] = IG_ETL_CH_HOST
if IG_ETL_CH_PORT:
    _container_env["CH_PORT"] = IG_ETL_CH_PORT
if IG_ETL_CH_SECURE:
    _container_env["CH_SECURE"] = IG_ETL_CH_SECURE


with DAG(
    dag_id="ig_etl_daily",
    description="Daily Instagram ETL sync via ig-etl container image",
    schedule=IG_ETL_DAG_CRON,
    start_date=pendulum.datetime(2026, 4, 23, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["ig", "etl", "clickhouse"],
) as dag:
    run_ig_sync = DockerOperator(
        task_id="run_ig_sync",
        image=IG_ETL_IMAGE,
        command=["--env-file", CONTAINER_ENV_FILE_PATH],
        docker_url="unix:///var/run/docker.sock",
        network_mode=IG_ETL_DOCKER_NETWORK_MODE,
        extra_hosts=_extra_hosts,
        environment=_container_env or None,
        mounts=[
            Mount(
                source=IG_ETL_ENV_FILE_HOST_PATH,
                target=CONTAINER_ENV_FILE_PATH,
                type="bind",
                read_only=True,
            )
        ],
        mount_tmp_dir=False,
        force_pull=False,
        do_xcom_push=False,
    )
