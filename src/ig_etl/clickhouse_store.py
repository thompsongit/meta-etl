from __future__ import annotations

from datetime import datetime
from typing import Any


def load_state(
    ch_client: Any,
    account_id: str,
    stream: str,
) -> tuple[str | None, datetime | None, int]:
    query = """
        SELECT cursor_value, cursor_ts, lookback_hours
        FROM etl_state
        WHERE account_id = {account_id:String}
          AND stream = {stream:String}
        ORDER BY updated_at DESC
        LIMIT 1
    """
    result = ch_client.query(
        query,
        parameters={
            "account_id": account_id,
            "stream": stream,
        },
    ).result_rows
    if not result:
        return None, None, 72
    cursor_value, cursor_ts, lookback_hours = result[0]
    return cursor_value, cursor_ts, int(lookback_hours)


def insert_rows(
    ch_client: Any,
    table: str,
    columns: list[str],
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0
    ch_client.insert(table, rows, column_names=columns)
    return len(rows)


def get_recent_media_ids(
    ch_client: Any,
    ig_user_id: str,
    limit: int,
) -> list[str]:
    if limit <= 0:
        return []
    query = """
        SELECT ig_media_id
        FROM curated_ig_media_current
        WHERE ig_user_id = {ig_user_id:String}
        ORDER BY coalesce(source_timestamp, source_updated_at, version_ts) DESC
        LIMIT {media_limit:UInt32}
    """
    result = ch_client.query(
        query,
        parameters={
            "ig_user_id": ig_user_id,
            "media_limit": limit,
        },
    ).result_rows
    return [row[0] for row in result if row and row[0]]


def get_recent_media_ids_for_comments(
    ch_client: Any,
    ig_user_id: str,
    limit: int,
) -> list[str]:
    return get_recent_media_ids(ch_client, ig_user_id, limit)


def insert_sync_run(
    ch_client: Any,
    run_id: str,
    account_id: str,
    stream: str,
    run_type: str,
    status: str,
    rows_extracted: int,
    rows_loaded_raw: int,
    rows_loaded_curated: int,
    error_message: str | None,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    insert_rows(
        ch_client,
        "etl_sync_runs",
        [
            "run_id",
            "account_id",
            "stream",
            "run_type",
            "status",
            "rows_extracted",
            "rows_loaded_raw",
            "rows_loaded_curated",
            "error_message",
            "started_at",
            "finished_at",
        ],
        [
            (
                run_id,
                account_id,
                stream,
                run_type,
                status,
                rows_extracted,
                rows_loaded_raw,
                rows_loaded_curated,
                error_message,
                started_at,
                finished_at,
            )
        ],
    )
