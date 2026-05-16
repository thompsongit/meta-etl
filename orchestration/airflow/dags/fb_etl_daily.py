from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


FB_ETL_IMAGE = os.getenv("FB_ETL_IMAGE", "fb-etl:dev-local")
FB_ETL_ENV_FILE_HOST_PATH = os.getenv(
    "FB_ETL_ENV_FILE_HOST_PATH",
    "/home/dev/meta-etl/.prod.fb.env",
)
FB_ETL_DAG_CRON = os.getenv("FB_ETL_DAG_CRON", "0 5 * * *")
FB_ETL_DOCKER_NETWORK_MODE = os.getenv("FB_ETL_DOCKER_NETWORK_MODE", "bridge")
FB_ETL_CH_HOST = os.getenv("FB_ETL_CH_HOST", "").strip()
FB_ETL_CH_PORT = os.getenv("FB_ETL_CH_PORT", "").strip()
FB_ETL_CH_SECURE = os.getenv("FB_ETL_CH_SECURE", "").strip()
FB_ETL_CH_CLUSTER = os.getenv("FB_ETL_CH_CLUSTER", "").strip()
FB_ETL_CH_ALT_HOSTS = os.getenv("FB_ETL_CH_ALT_HOSTS", "").strip()

_extra_hosts: dict[str, str] = {}
extra_host_mapping = os.getenv("FB_ETL_EXTRA_HOST_MAPPING", "host.docker.internal:host-gateway")
if extra_host_mapping and ":" in extra_host_mapping:
    host, target = extra_host_mapping.split(":", 1)
    host = host.strip()
    target = target.strip()
    if host and target:
        _extra_hosts[host] = target

CONTAINER_ENV_FILE_PATH = "/run/config/fb_prod_env"
_container_env: dict[str, str] = {}
if FB_ETL_CH_HOST:
    _container_env["CH_HOST"] = FB_ETL_CH_HOST
if FB_ETL_CH_PORT:
    _container_env["CH_PORT"] = FB_ETL_CH_PORT
if FB_ETL_CH_SECURE:
    _container_env["CH_SECURE"] = FB_ETL_CH_SECURE
if FB_ETL_CH_CLUSTER:
    _container_env["CH_CLUSTER"] = FB_ETL_CH_CLUSTER
if FB_ETL_CH_ALT_HOSTS:
    _container_env["CH_ALT_HOSTS"] = FB_ETL_CH_ALT_HOSTS


with DAG(
    dag_id="fb_etl_daily",
    description="Daily Facebook Page ETL sync via fb-etl container image",
    schedule=FB_ETL_DAG_CRON,
    start_date=pendulum.datetime(2026, 5, 13, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "etl",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["fb", "etl", "clickhouse"],
) as dag:
    run_fb_sync = DockerOperator(
        task_id="run_fb_sync",
        image=FB_ETL_IMAGE,
        command=["--env-file", CONTAINER_ENV_FILE_PATH],
        docker_url="unix:///var/run/docker.sock",
        network_mode=FB_ETL_DOCKER_NETWORK_MODE,
        extra_hosts=_extra_hosts,
        environment=_container_env or None,
        mounts=[
            Mount(
                source=FB_ETL_ENV_FILE_HOST_PATH,
                target=CONTAINER_ENV_FILE_PATH,
                type="bind",
                read_only=True,
            )
        ],
        mount_tmp_dir=False,
        force_pull=False,
        do_xcom_push=False,
    )
