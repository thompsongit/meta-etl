from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Any

from .clickhouse_store import (
    claim_stream_window,
    complete_stream_window,
    get_recent_media_ids,
    insert_rows,
    insert_run_step,
    insert_sync_run,
    load_state,
)
from .config import SyncConfig
from .constants import (
    MEDIA_FIELDS_CANDIDATES,
    MEDIA_INSIGHT_CANDIDATES,
    STREAM_NAME,
    USER_INSIGHT_CANDIDATES,
)
from .graph_api import (
    GraphAPIError,
    fetch_comments_for_media,
    graph_get_json,
    is_permission_error,
    iter_graph_collection,
    try_metric_candidates,
)
from .lock import acquire_nonblocking_lock
from .models import SyncCounters, SyncWindow
from .transform import (
    build_comment_rows_for_media,
    build_media_rows,
    build_profile_rows,
    flatten_insight_rows,
)
from .utils import parse_graph_timestamp, utc_now


def _build_windows(
    start_dt: datetime,
    end_dt: datetime,
    chunk_days: int,
    max_windows_per_run: int,
) -> list[SyncWindow]:
    if start_dt >= end_dt:
        return []

    windows: list[SyncWindow] = []
    chunk = timedelta(days=max(1, chunk_days))
    cursor = start_dt

    while cursor < end_dt:
        next_cursor = min(cursor + chunk, end_dt)
        windows.append(SyncWindow(start=cursor, end=next_cursor))
        cursor = next_cursor
        if max_windows_per_run > 0 and len(windows) >= max_windows_per_run:
            break

    return windows


def _plan_media_windows(
    config: SyncConfig,
    media_cursor_ts: datetime | None,
    media_lookback: int,
) -> tuple[list[SyncWindow], str, str]:
    now_dt = utc_now()
    catchup_threshold = timedelta(days=max(1, config.backfill_chunk_days))

    if media_cursor_ts is None:
        start_dt = config.initial_sync_start_at
        if start_dt is None:
            start_dt = now_dt - timedelta(days=config.backfill_days)
        if start_dt > now_dt:
            raise ValueError(
                "INITIAL_SYNC_START_AT/--initial-sync-start-at must be <= current UTC time"
            )
        windows = _build_windows(
            start_dt,
            now_dt,
            config.backfill_chunk_days,
            config.max_windows_per_run,
        )
        return windows, "backfill", "bootstrap"

    if now_dt - media_cursor_ts > catchup_threshold:
        windows = _build_windows(
            media_cursor_ts,
            now_dt,
            config.backfill_chunk_days,
            config.max_windows_per_run,
        )
        return windows, "backfill", "catchup"

    since_dt = media_cursor_ts - timedelta(hours=media_lookback)
    return [SyncWindow(start=since_dt, end=now_dt)], "incremental", "incremental"


def _resolve_comments_since(
    window: SyncWindow,
    comments_cursor_ts: datetime | None,
    comments_lookback_hours: int,
    comments_backfill_days: int,
    run_type: str,
) -> datetime:
    if comments_cursor_ts is None:
        if run_type == "backfill":
            return window.start
        return window.end - timedelta(days=comments_backfill_days)

    if run_type == "backfill":
        return window.start

    cursor_since = comments_cursor_ts - timedelta(hours=comments_lookback_hours)
    return max(window.start, cursor_since)


def _fetch_profile_payload(
    http_client: Any,
    config: SyncConfig,
) -> dict[str, Any]:
    profile_fields_candidates = [
        "id,legacy_instagram_user_id,username,name,biography,website,profile_picture_url,followers_count,follows_count,media_count",
        "id,username,name,biography,website,profile_picture_url,followers_count,follows_count,media_count",
    ]
    for fields in profile_fields_candidates:
        try:
            return graph_get_json(
                http_client,
                config.graph_base,
                config.graph_version,
                config.graph_token,
                f"/{config.ig_user_id}",
                params={"fields": fields},
            )
        except GraphAPIError as exc:
            message = exc.message.lower()
            if exc.code == 100 and (
                "legacy_instagram_user_id" in message
                or "ig_id" in message
                or "nonexisting field" in message
            ):
                continue
            raise
    raise RuntimeError("Unable to fetch profile fields for IG user")


def _fetch_media_items_for_window(
    http_client: Any,
    config: SyncConfig,
    window: SyncWindow,
) -> list[dict[str, Any]]:
    media_since_unix = int(window.start.timestamp())
    media_until_unix = int(window.end.timestamp())

    media_items: list[dict[str, Any]] | None = None
    media_last_exc: GraphAPIError | None = None
    fallback_without_until = False

    for fields in MEDIA_FIELDS_CANDIDATES:
        params: dict[str, Any] = {
            "fields": fields,
            "limit": config.media_page_size,
            "since": media_since_unix,
            "until": media_until_unix,
        }
        try:
            media_items = iter_graph_collection(
                http_client,
                config.graph_base,
                config.graph_version,
                config.graph_token,
                f"/{config.ig_user_id}/media",
                params,
            )
            break
        except GraphAPIError as exc:
            media_last_exc = exc
            message = exc.message.lower()
            if exc.code == 100 and "until" in message:
                fallback_params = {
                    "fields": fields,
                    "limit": config.media_page_size,
                    "since": media_since_unix,
                }
                media_items = iter_graph_collection(
                    http_client,
                    config.graph_base,
                    config.graph_version,
                    config.graph_token,
                    f"/{config.ig_user_id}/media",
                    fallback_params,
                )
                fallback_without_until = True
                break
            if exc.code == 100 and ("field" in message or "nonexisting field" in message):
                print(f"[WARN] media fields not supported, retrying with fallback set: {exc}")
                continue
            raise

    if media_items is None:
        if media_last_exc is not None:
            raise media_last_exc
        media_items = []

    if fallback_without_until:
        filtered_items: list[dict[str, Any]] = []
        for media in media_items:
            media_ts = parse_graph_timestamp(media.get("timestamp"))
            if media_ts is None or media_ts <= window.end:
                filtered_items.append(media)
        media_items = filtered_items

    return media_items


def _sync_window(
    config: SyncConfig,
    ch_client: Any,
    http_client: Any,
    run_id: str,
    run_type: str,
    window: SyncWindow,
    media_lookback: int,
    comments_lookback: int,
    comments_cursor_ts: datetime | None,
) -> tuple[SyncCounters, bool]:
    counters = SyncCounters()
    ingested_at = utc_now()

    comments_since_dt = _resolve_comments_since(
        window,
        comments_cursor_ts,
        comments_lookback,
        config.comments_backfill_days,
        run_type,
    )
    comments_since_unix = int(comments_since_dt.timestamp())
    comments_until_unix = int(window.end.timestamp())

    profile_payload = _fetch_profile_payload(http_client, config)
    counters.rows_extracted += 1

    raw_profile_rows, curated_profile_rows = build_profile_rows(
        config.ig_user_id, profile_payload, run_id, ingested_at
    )
    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_ig_user_profile",
        [
            "ig_user_id",
            "ig_id",
            "username",
            "name",
            "biography",
            "website",
            "profile_picture_url",
            "followers_count",
            "follows_count",
            "media_count",
            "source_updated_at",
            "payload_json",
            "run_id",
            "ingested_at",
        ],
        raw_profile_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_ig_user_profile_current",
        [
            "ig_user_id",
            "ig_id",
            "username",
            "name",
            "biography",
            "website",
            "profile_picture_url",
            "followers_count",
            "follows_count",
            "media_count",
            "source_updated_at",
            "version_ts",
        ],
        curated_profile_rows,
    )

    media_items = _fetch_media_items_for_window(http_client, config, window)
    counters.rows_extracted += len(media_items)
    print(
        f"[INFO] fetched media rows={len(media_items)} "
        f"window={window.start.isoformat()}..{window.end.isoformat()}"
    )

    media_rows = build_media_rows(
        config.ig_user_id,
        media_items,
        run_id,
        ingested_at,
    )
    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_ig_media",
        [
            "ig_user_id",
            "ig_media_id",
            "media_type",
            "media_product_type",
            "permalink",
            "media_url",
            "thumbnail_url",
            "caption",
            "username",
            "is_comment_enabled",
            "like_count",
            "comments_count",
            "source_timestamp",
            "source_updated_at",
            "payload_json",
            "run_id",
            "ingested_at",
        ],
        media_rows.raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_ig_media_current",
        [
            "ig_user_id",
            "ig_media_id",
            "media_type",
            "media_product_type",
            "permalink",
            "media_url",
            "thumbnail_url",
            "caption",
            "username",
            "is_comment_enabled",
            "like_count",
            "comments_count",
            "source_timestamp",
            "source_updated_at",
            "version_ts",
        ],
        media_rows.curated_rows,
    )

    user_insight_items = try_metric_candidates(
        http_client,
        config.graph_base,
        config.graph_version,
        config.graph_token,
        f"/{config.ig_user_id}/insights",
        USER_INSIGHT_CANDIDATES,
    )
    user_raw_rows, user_curated_rows = flatten_insight_rows(
        config.ig_user_id,
        None,
        user_insight_items,
        run_id,
        ingested_at,
    )
    counters.rows_extracted += len(user_insight_items)
    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_ig_user_insights",
        [
            "ig_user_id",
            "metric",
            "period",
            "end_time",
            "breakdown_key",
            "metric_value_float",
            "metric_value_json",
            "title",
            "description",
            "source_updated_at",
            "payload_json",
            "run_id",
            "ingested_at",
        ],
        user_raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_ig_user_insights_timeseries",
        [
            "ig_user_id",
            "metric",
            "period",
            "end_time",
            "breakdown_key",
            "metric_value_float",
            "metric_value_json",
            "version_ts",
        ],
        user_curated_rows,
    )

    insight_seed_media_ids: list[str] = []
    if run_type == "incremental":
        insight_seed_media_ids = get_recent_media_ids(
            ch_client,
            config.ig_user_id,
            config.max_media_insight_requests,
        )

    media_ids_for_insights = list(dict.fromkeys(media_rows.media_ids + insight_seed_media_ids))
    if config.max_media_insight_requests > 0:
        media_ids_for_insights = media_ids_for_insights[: config.max_media_insight_requests]

    media_insight_raw_rows: list[tuple[Any, ...]] = []
    media_insight_curated_rows: list[tuple[Any, ...]] = []
    processed_media = 0
    for media_id in media_ids_for_insights:
        insight_items = try_metric_candidates(
            http_client,
            config.graph_base,
            config.graph_version,
            config.graph_token,
            f"/{media_id}/insights",
            MEDIA_INSIGHT_CANDIDATES,
        )
        counters.rows_extracted += len(insight_items)
        raw_rows, curated_rows = flatten_insight_rows(
            config.ig_user_id,
            media_id,
            insight_items,
            run_id,
            ingested_at,
        )
        media_insight_raw_rows.extend(raw_rows)
        media_insight_curated_rows.extend(curated_rows)
        processed_media += 1

    print(f"[INFO] media insight probes={processed_media}")

    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_ig_media_insights",
        [
            "ig_user_id",
            "ig_media_id",
            "metric",
            "period",
            "end_time",
            "breakdown_key",
            "metric_value_float",
            "metric_value_json",
            "title",
            "description",
            "source_updated_at",
            "payload_json",
            "run_id",
            "ingested_at",
        ],
        media_insight_raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_ig_media_insights_timeseries",
        [
            "ig_user_id",
            "ig_media_id",
            "metric",
            "period",
            "end_time",
            "breakdown_key",
            "metric_value_float",
            "metric_value_json",
            "version_ts",
        ],
        media_insight_curated_rows,
    )

    comments_permission_skipped = False
    comments_media_probed = 0
    comment_raw_rows: list[tuple[Any, ...]] = []
    comment_curated_rows: list[tuple[Any, ...]] = []

    if config.disable_comments:
        print("[INFO] comments sync disabled by flag")
    else:
        seed_media_ids: list[str] = []
        if run_type == "incremental":
            seed_media_ids = get_recent_media_ids(
                ch_client, config.ig_user_id, config.comments_media_scan_limit
            )

        media_ids_to_scan = list(dict.fromkeys(media_rows.media_ids + seed_media_ids))
        if config.comments_media_scan_limit > 0:
            media_ids_to_scan = media_ids_to_scan[: config.comments_media_scan_limit]
        print(f"[INFO] comment media targets={len(media_ids_to_scan)}")

        seen_comment_ids: set[str] = set()
        for media_id in media_ids_to_scan:
            try:
                comment_items = fetch_comments_for_media(
                    http_client,
                    config.graph_base,
                    config.graph_version,
                    config.graph_token,
                    media_id,
                    config.comments_page_size,
                    comments_since_unix,
                    comments_until_unix,
                )
            except GraphAPIError as exc:
                if is_permission_error(exc):
                    comments_permission_skipped = True
                    print(
                        "[WARN] comment scope unavailable; skipping comments stream: "
                        f"{exc}"
                    )
                    break
                if exc.code == 100:
                    print(f"[WARN] skipping comments for media {media_id}: {exc}")
                    continue
                raise

            comments_media_probed += 1
            counters.rows_extracted += len(comment_items)

            comment_rows = build_comment_rows_for_media(
                config.ig_user_id,
                media_id,
                comment_items,
                run_id,
                ingested_at,
                seen_comment_ids,
            )
            comment_raw_rows.extend(comment_rows.raw_rows)
            comment_curated_rows.extend(comment_rows.curated_rows)

        print(f"[INFO] comment probes completed={comments_media_probed}")

    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_ig_comments",
        [
            "ig_user_id",
            "ig_media_id",
            "ig_comment_id",
            "parent_comment_id",
            "text",
            "username",
            "like_count",
            "hidden",
            "source_timestamp",
            "source_updated_at",
            "payload_json",
            "run_id",
            "ingested_at",
        ],
        comment_raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_ig_comments_current",
        [
            "ig_comment_id",
            "ig_user_id",
            "ig_media_id",
            "parent_comment_id",
            "text",
            "username",
            "like_count",
            "hidden",
            "source_timestamp",
            "source_updated_at",
            "version_ts",
        ],
        comment_curated_rows,
    )

    state_rows = [
        (
            config.ig_user_id,
            "ig_media",
            window.end.isoformat(),
            window.end,
            media_lookback,
            run_id,
            "{}",
            utc_now(),
        )
    ]
    if not config.disable_comments and not comments_permission_skipped:
        state_rows.append(
            (
                config.ig_user_id,
                "ig_comments",
                window.end.isoformat(),
                window.end,
                comments_lookback,
                run_id,
                "{}",
                utc_now(),
            )
        )

    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "etl_state",
        [
            "account_id",
            "stream",
            "cursor_value",
            "cursor_ts",
            "lookback_hours",
            "last_successful_run_id",
            "metadata_json",
            "updated_at",
        ],
        state_rows,
    )

    return counters, comments_permission_skipped


def run_sync(
    config: SyncConfig,
    clickhouse_connect_module: Any,
    httpx_module: Any,
) -> int:
    run_id = str(uuid.uuid4())
    started_at = utc_now()
    run_type = "incremental"
    mode = "incremental"
    counters = SyncCounters()

    lock_handle = acquire_nonblocking_lock(config.lock_file)
    if lock_handle is None:
        print(f"[INFO] another sync is already running (lock={config.lock_file}); exiting")
        return 0

    ch_client = None
    http_client = None
    try:
        ch_client = clickhouse_connect_module.get_client(
            host=config.ch_host,
            port=config.ch_port,
            username=config.ch_username,
            password=config.ch_password,
            database=config.ch_database,
            secure=config.ch_secure,
        )
        http_client = httpx_module.Client(timeout=config.http_timeout_seconds)

        print(f"[INFO] run_id={run_id}")
        print(f"[INFO] loading stream state for account={config.ig_user_id}")
        print(f"[INFO] graph_base={config.graph_base} graph_version={config.graph_version}")

        _, media_cursor_ts, media_stored_lookback = load_state(
            ch_client, config.ig_user_id, "ig_media"
        )
        _, comments_cursor_ts, comments_stored_lookback = load_state(
            ch_client, config.ig_user_id, "ig_comments"
        )

        media_lookback = (
            config.lookback_hours if config.lookback_hours > 0 else media_stored_lookback
        )
        comments_lookback = (
            config.comments_lookback_hours
            if config.comments_lookback_hours > 0
            else comments_stored_lookback
        )

        windows, run_type, mode = _plan_media_windows(
            config,
            media_cursor_ts,
            media_lookback,
        )

        print(
            f"[INFO] mode={mode} run_type={run_type} windows={len(windows)} "
            f"chunk_days={config.backfill_chunk_days}"
        )
        if windows:
            print(
                f"[INFO] first_window={windows[0].start.isoformat()}..{windows[0].end.isoformat()} "
                f"last_window={windows[-1].start.isoformat()}..{windows[-1].end.isoformat()}"
            )

        insert_run_step(
            ch_client,
            run_id=run_id,
            account_id=config.ig_user_id,
            stream=STREAM_NAME,
            window_id=None,
            step="plan_windows",
            status="success",
            message=(
                f"mode={mode}; run_type={run_type}; windows={len(windows)}; "
                f"chunk_days={config.backfill_chunk_days}"
            ),
        )

        current_comments_cursor_ts = comments_cursor_ts
        for idx, window in enumerate(windows, start=1):
            should_process, window_id, attempt = claim_stream_window(
                ch_client,
                account_id=config.ig_user_id,
                stream="ig_media",
                window_start=window.start,
                window_end=window.end,
                run_id=run_id,
            )
            if not should_process:
                print(
                    f"[INFO] window {idx}/{len(windows)} already completed; "
                    f"skipping window_id={window_id}"
                )
                insert_run_step(
                    ch_client,
                    run_id=run_id,
                    account_id=config.ig_user_id,
                    stream=STREAM_NAME,
                    window_id=window_id,
                    step="window_skip",
                    status="success",
                    message=(
                        f"window={window.start.isoformat()}..{window.end.isoformat()} "
                        "already marked success"
                    ),
                )
                continue

            print(
                f"[INFO] processing window {idx}/{len(windows)} attempt={attempt} "
                f"window={window.start.isoformat()}..{window.end.isoformat()}"
            )
            insert_run_step(
                ch_client,
                run_id=run_id,
                account_id=config.ig_user_id,
                stream=STREAM_NAME,
                window_id=window_id,
                step="window_start",
                status="running",
                message=(
                    f"attempt={attempt}; window={window.start.isoformat()}.."
                    f"{window.end.isoformat()}"
                ),
            )

            try:
                window_counters, comments_permission_skipped = _sync_window(
                    config=config,
                    ch_client=ch_client,
                    http_client=http_client,
                    run_id=run_id,
                    run_type=run_type,
                    window=window,
                    media_lookback=media_lookback,
                    comments_lookback=comments_lookback,
                    comments_cursor_ts=current_comments_cursor_ts,
                )

                counters.rows_extracted += window_counters.rows_extracted
                counters.rows_loaded_raw += window_counters.rows_loaded_raw
                counters.rows_loaded_curated += window_counters.rows_loaded_curated

                complete_stream_window(
                    ch_client,
                    account_id=config.ig_user_id,
                    stream="ig_media",
                    window_start=window.start,
                    window_end=window.end,
                    window_id=window_id,
                    attempt=attempt,
                    run_id=run_id,
                    status="success",
                    rows_extracted=window_counters.rows_extracted,
                    rows_loaded_raw=window_counters.rows_loaded_raw,
                    rows_loaded_curated=window_counters.rows_loaded_curated,
                    error_message=None,
                )
                insert_run_step(
                    ch_client,
                    run_id=run_id,
                    account_id=config.ig_user_id,
                    stream=STREAM_NAME,
                    window_id=window_id,
                    step="window_finish",
                    status="success",
                    message=(
                        f"rows_extracted={window_counters.rows_extracted}; "
                        f"rows_loaded_raw={window_counters.rows_loaded_raw}; "
                        f"rows_loaded_curated={window_counters.rows_loaded_curated}"
                    ),
                )

                if not config.disable_comments and not comments_permission_skipped:
                    current_comments_cursor_ts = window.end

            except Exception as exc:  # noqa: BLE001
                error_message = str(exc)[:4000]
                complete_stream_window(
                    ch_client,
                    account_id=config.ig_user_id,
                    stream="ig_media",
                    window_start=window.start,
                    window_end=window.end,
                    window_id=window_id,
                    attempt=attempt,
                    run_id=run_id,
                    status="failed",
                    rows_extracted=0,
                    rows_loaded_raw=0,
                    rows_loaded_curated=0,
                    error_message=error_message,
                )
                insert_run_step(
                    ch_client,
                    run_id=run_id,
                    account_id=config.ig_user_id,
                    stream=STREAM_NAME,
                    window_id=window_id,
                    step="window_finish",
                    status="failed",
                    message=error_message,
                )
                raise

        finished_at = utc_now()
        insert_sync_run(
            ch_client=ch_client,
            run_id=run_id,
            account_id=config.ig_user_id,
            stream=STREAM_NAME,
            run_type=run_type,
            status="success",
            rows_extracted=counters.rows_extracted,
            rows_loaded_raw=counters.rows_loaded_raw,
            rows_loaded_curated=counters.rows_loaded_curated,
            error_message=None,
            started_at=started_at,
            finished_at=finished_at,
        )

        print("[INFO] sync completed")
        print(f"[INFO] rows_extracted={counters.rows_extracted}")
        print(f"[INFO] rows_loaded_raw={counters.rows_loaded_raw}")
        print(f"[INFO] rows_loaded_curated={counters.rows_loaded_curated}")
        return 0

    except Exception as exc:  # noqa: BLE001
        finished_at = utc_now()
        error_message = str(exc)[:4000]
        if ch_client is not None:
            try:
                insert_sync_run(
                    ch_client=ch_client,
                    run_id=run_id,
                    account_id=config.ig_user_id,
                    stream=STREAM_NAME,
                    run_type=run_type,
                    status="failed",
                    rows_extracted=counters.rows_extracted,
                    rows_loaded_raw=counters.rows_loaded_raw,
                    rows_loaded_curated=counters.rows_loaded_curated,
                    error_message=error_message,
                    started_at=started_at,
                    finished_at=finished_at,
                )
            except Exception:
                pass
        print(f"[ERROR] sync failed: {exc}")
        return 1
    finally:
        if http_client is not None:
            http_client.close()
        if ch_client is not None:
            ch_client.close()
        lock_handle.close()
