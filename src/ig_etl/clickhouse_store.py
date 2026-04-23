from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any


def _as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


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
    return cursor_value, _as_utc(cursor_ts), int(lookback_hours)


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


def _build_window_id(
    account_id: str,
    stream: str,
    window_start: datetime,
    window_end: datetime,
) -> str:
    seed = (
        f"{account_id}|{stream}|"
        f"{window_start.isoformat()}|{window_end.isoformat()}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))


def claim_stream_window(
    ch_client: Any,
    account_id: str,
    stream: str,
    window_start: datetime,
    window_end: datetime,
    run_id: str,
) -> tuple[bool, str, int]:
    query = """
        SELECT status, attempt, window_id
        FROM etl_stream_windows
        WHERE account_id = {account_id:String}
          AND stream = {stream:String}
          AND window_start = {window_start:DateTime64(3)}
          AND window_end = {window_end:DateTime64(3)}
        ORDER BY updated_at DESC
        LIMIT 1
    """
    result = ch_client.query(
        query,
        parameters={
            "account_id": account_id,
            "stream": stream,
            "window_start": window_start,
            "window_end": window_end,
        },
    ).result_rows

    window_id = _build_window_id(account_id, stream, window_start, window_end)
    next_attempt = 1
    if result:
        status, attempt, stored_window_id = result[0]
        if stored_window_id:
            window_id = stored_window_id
        next_attempt = int(attempt or 0) + 1
        if status == "success":
            return False, window_id, int(attempt or 1)

    insert_rows(
        ch_client,
        "etl_stream_windows",
        [
            "account_id",
            "stream",
            "window_start",
            "window_end",
            "window_id",
            "attempt",
            "run_id",
            "status",
            "rows_extracted",
            "rows_loaded_raw",
            "rows_loaded_curated",
            "error_message",
            "updated_at",
        ],
        [
            (
                account_id,
                stream,
                window_start,
                window_end,
                window_id,
                next_attempt,
                run_id,
                "running",
                0,
                0,
                0,
                None,
                datetime.now(timezone.utc),
            )
        ],
    )
    return True, window_id, next_attempt


def complete_stream_window(
    ch_client: Any,
    account_id: str,
    stream: str,
    window_start: datetime,
    window_end: datetime,
    window_id: str,
    attempt: int,
    run_id: str,
    status: str,
    rows_extracted: int,
    rows_loaded_raw: int,
    rows_loaded_curated: int,
    error_message: str | None,
) -> None:
    insert_rows(
        ch_client,
        "etl_stream_windows",
        [
            "account_id",
            "stream",
            "window_start",
            "window_end",
            "window_id",
            "attempt",
            "run_id",
            "status",
            "rows_extracted",
            "rows_loaded_raw",
            "rows_loaded_curated",
            "error_message",
            "updated_at",
        ],
        [
            (
                account_id,
                stream,
                window_start,
                window_end,
                window_id,
                attempt,
                run_id,
                status,
                max(0, rows_extracted),
                max(0, rows_loaded_raw),
                max(0, rows_loaded_curated),
                error_message,
                datetime.now(timezone.utc),
            )
        ],
    )


def insert_run_step(
    ch_client: Any,
    run_id: str,
    account_id: str,
    stream: str,
    window_id: str | None,
    step: str,
    status: str,
    message: str | None = None,
) -> None:
    insert_rows(
        ch_client,
        "etl_run_steps",
        [
            "run_id",
            "account_id",
            "stream",
            "window_id",
            "step",
            "status",
            "message",
            "created_at",
        ],
        [
            (
                run_id,
                account_id,
                stream,
                window_id,
                step,
                status,
                message,
                datetime.now(timezone.utc),
            )
        ],
    )


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
