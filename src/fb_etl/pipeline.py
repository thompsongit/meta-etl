from __future__ import annotations
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Any

from .clickhouse_store import (
    claim_stream_window,
    complete_stream_window,
    get_recent_post_ids,
    insert_rows,
    insert_run_step,
    insert_sync_run,
    load_state,
)
from .config import SyncConfig
from .constants import (
    COMMENT_FIELDS_CANDIDATES,
    COMMENTS_CURSOR_STREAM,
    PAGE_FIELDS_CANDIDATES,
    PAGE_INSIGHT_METRICS,
    POST_FIELDS_CANDIDATES,
    POSTS_CURSOR_STREAM,
    POST_INSIGHT_METRICS,
    STREAM_NAME,
)
from .graph_api import (
    GraphAPIError,
    graph_get_json,
    is_field_validation_error,
    is_metric_validation_error,
    is_permission_error,
    iter_graph_collection,
)
from .lock import acquire_nonblocking_lock
from .models import SyncCounters, SyncWindow
from .transform import (
    build_comment_rows_for_post,
    build_page_profile_rows,
    build_post_rows,
    flatten_insight_rows,
)
from .utils import utc_now


def _parse_host_port(value: str, default_port: int) -> tuple[str, int]:
    target = value.strip()
    if not target:
        raise ValueError("Empty ClickHouse host target in CH_ALT_HOSTS")

    if target.startswith("[") and "]" in target:
        end = target.find("]")
        host = target[1:end].strip()
        remainder = target[end + 1 :].strip()
        if remainder.startswith(":") and remainder[1:].isdigit():
            return host, int(remainder[1:])
        return host, default_port

    host_part, sep, port_part = target.rpartition(":")
    if sep and host_part and port_part.isdigit():
        return host_part.strip(), int(port_part)
    return target, default_port


def _clickhouse_targets(config: SyncConfig) -> list[tuple[str, int]]:
    candidates: list[tuple[str, int]] = [(config.ch_host, config.ch_port)]
    for alt in config.ch_alt_hosts:
        candidates.append(_parse_host_port(alt, config.ch_port))

    deduped: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for host, port in candidates:
        normalized = (host.strip(), int(port))
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _verify_clickhouse_cluster(ch_client: Any, cluster_name: str) -> None:
    rows = ch_client.query(
        """
        SELECT count()
        FROM system.clusters
        WHERE cluster = {cluster:String}
        """,
        parameters={"cluster": cluster_name},
    ).result_rows
    count = int(rows[0][0]) if rows else 0
    if count <= 0:
        raise ValueError(
            f"Configured CH_CLUSTER={cluster_name!r} not found in system.clusters"
        )


def _connect_clickhouse(config: SyncConfig, clickhouse_connect_module: Any) -> Any:
    targets = _clickhouse_targets(config)
    last_exc: Exception | None = None

    for host, port in targets:
        ch_client = None
        try:
            ch_client = clickhouse_connect_module.get_client(
                host=host,
                port=port,
                username=config.ch_username,
                password=config.ch_password,
                database=config.ch_database,
                secure=config.ch_secure,
            )
            ch_client.command("SELECT 1")
            if config.ch_cluster:
                _verify_clickhouse_cluster(ch_client, config.ch_cluster)
                print(
                    f"[INFO] clickhouse connected host={host}:{port} "
                    f"cluster={config.ch_cluster}"
                )
            elif len(targets) > 1:
                print(f"[INFO] clickhouse connected host={host}:{port}")
            return ch_client
        except Exception as exc:  # pragma: no cover - runtime/network path
            last_exc = exc
            print(f"[WARN] clickhouse connect failed host={host}:{port}: {exc}")
            if ch_client is not None:
                try:
                    ch_client.close()
                except Exception:
                    pass

    target_text = ",".join(f"{host}:{port}" for host, port in targets)
    raise RuntimeError(f"Unable to connect to ClickHouse targets: {target_text}") from last_exc


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


def _plan_post_windows(
    config: SyncConfig,
    posts_cursor_ts: datetime | None,
    posts_lookback: int,
) -> tuple[list[SyncWindow], str, str]:
    now_dt = utc_now()
    catchup_threshold = timedelta(days=max(1, config.backfill_chunk_days))

    if posts_cursor_ts is None:
        start_dt = config.initial_sync_start_at
        if start_dt is None:
            start_dt = now_dt - timedelta(days=config.backfill_days)
        if start_dt > now_dt:
            raise ValueError(
                "FB_INITIAL_SYNC_START_AT/--initial-sync-start-at must be <= current UTC time"
            )
        windows = _build_windows(
            start_dt,
            now_dt,
            config.backfill_chunk_days,
            config.max_windows_per_run,
        )
        return windows, "backfill", "bootstrap"

    if now_dt - posts_cursor_ts > catchup_threshold:
        windows = _build_windows(
            posts_cursor_ts,
            now_dt,
            config.backfill_chunk_days,
            config.max_windows_per_run,
        )
        return windows, "backfill", "catchup"

    since_dt = posts_cursor_ts - timedelta(hours=posts_lookback)
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


def _merge_probe_ids(primary_ids: list[str], secondary_ids: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for post_id in primary_ids + secondary_ids:
        if not post_id or post_id in seen:
            continue
        seen.add(post_id)
        merged.append(post_id)
    return merged


def _fetch_page_profile_payload(http_client: Any, config: SyncConfig) -> dict[str, Any]:
    last_exc: GraphAPIError | None = None
    for fields in PAGE_FIELDS_CANDIDATES:
        try:
            return graph_get_json(
                http_client,
                config.graph_base,
                config.graph_version,
                config.graph_token,
                f"/{config.page_id}",
                params={"fields": fields},
            )
        except GraphAPIError as exc:
            last_exc = exc
            if is_field_validation_error(exc):
                continue
            raise
    if last_exc is not None:
        raise last_exc
    return {}


def _fetch_collection_with_candidates(
    http_client: Any,
    config: SyncConfig,
    path: str,
    field_candidates: list[str],
    limit: int,
    since_unix: int | None = None,
    until_unix: int | None = None,
) -> list[dict[str, Any]]:
    last_exc: GraphAPIError | None = None

    for fields in field_candidates:
        candidate_params: list[dict[str, Any]] = []
        base = {"fields": fields, "limit": limit}
        if since_unix is not None and until_unix is not None:
            candidate_params.append({**base, "since": since_unix, "until": until_unix})
        if since_unix is not None:
            candidate_params.append({**base, "since": since_unix})
        if until_unix is not None:
            candidate_params.append({**base, "until": until_unix})
        candidate_params.append(base)

        for params in candidate_params:
            try:
                return iter_graph_collection(
                    http_client,
                    config.graph_base,
                    config.graph_version,
                    config.graph_token,
                    path,
                    params=params,
                )
            except GraphAPIError as exc:
                last_exc = exc
                if is_field_validation_error(exc) or is_metric_validation_error(exc):
                    continue
                raise

    if last_exc is not None:
        raise last_exc
    return []


def _fetch_posts_for_window(
    http_client: Any,
    config: SyncConfig,
    window_since_unix: int,
    window_until_unix: int,
) -> tuple[list[dict[str, Any]], str]:
    edge_paths = (
        f"/{config.page_id}/posts",
        f"/{config.page_id}/feed",
        f"/{config.page_id}/published_posts",
    )
    last_exc: GraphAPIError | None = None
    for path in edge_paths:
        try:
            rows = _fetch_collection_with_candidates(
                http_client=http_client,
                config=config,
                path=path,
                field_candidates=POST_FIELDS_CANDIDATES,
                limit=config.posts_page_size,
                since_unix=window_since_unix,
                until_unix=window_until_unix,
            )
            return rows, path
        except GraphAPIError as exc:
            last_exc = exc
            if is_permission_error(exc) or is_field_validation_error(exc):
                print(f"[WARN] post edge unavailable path={path}: {exc}")
                continue
            raise

    if last_exc is not None:
        raise last_exc
    return [], edge_paths[0]


def _fetch_insight_metric_rows(
    http_client: Any,
    config: SyncConfig,
    path: str,
    metric: str,
    period: str,
    since_unix: int | None = None,
    until_unix: int | None = None,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    base = {"metric": metric, "period": period}
    if since_unix is not None and until_unix is not None:
        candidates.append({**base, "since": since_unix, "until": until_unix})
    if since_unix is not None:
        candidates.append({**base, "since": since_unix})
    candidates.append(base)

    last_exc: GraphAPIError | None = None
    for params in candidates:
        try:
            payload = graph_get_json(
                http_client,
                config.graph_base,
                config.graph_version,
                config.graph_token,
                path,
                params=params,
            )
            rows = payload.get("data", [])
            return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        except GraphAPIError as exc:
            last_exc = exc
            if is_field_validation_error(exc) or is_metric_validation_error(exc):
                continue
            raise

    if last_exc is not None:
        raise last_exc
    return []


def _collect_page_insights_for_window(
    http_client: Any,
    config: SyncConfig,
    since_unix: int,
    until_unix: int,
) -> tuple[list[dict[str, Any]], bool]:
    if config.disable_page_insights:
        return [], False

    collected: list[dict[str, Any]] = []
    for metric in PAGE_INSIGHT_METRICS:
        try:
            rows = _fetch_insight_metric_rows(
                http_client=http_client,
                config=config,
                path=f"/{config.page_id}/insights",
                metric=metric,
                period="day",
                since_unix=since_unix,
                until_unix=until_unix,
            )
            if rows:
                collected.extend(rows)
        except GraphAPIError as exc:
            if is_permission_error(exc):
                print(f"[WARN] skipping page insights stream: {exc}")
                return collected, True
            if is_field_validation_error(exc) or is_metric_validation_error(exc):
                continue
            raise
    return collected, False


def _collect_post_insights_for_posts(
    http_client: Any,
    config: SyncConfig,
    post_ids: list[str],
) -> tuple[list[dict[str, Any]], bool]:
    if config.disable_post_insights:
        return [], False

    collected: list[dict[str, Any]] = []
    for post_id in post_ids[: max(1, config.max_post_insight_requests)]:
        for metric in POST_INSIGHT_METRICS:
            try:
                rows = _fetch_insight_metric_rows(
                    http_client=http_client,
                    config=config,
                    path=f"/{post_id}/insights",
                    metric=metric,
                    period="lifetime",
                )
                for row in rows:
                    row_copy = dict(row)
                    row_copy["_post_id"] = post_id
                    collected.append(row_copy)
            except GraphAPIError as exc:
                if is_permission_error(exc):
                    print(f"[WARN] skipping post insights stream: {exc}")
                    return collected, True
                if is_field_validation_error(exc) or is_metric_validation_error(exc):
                    continue
                raise
    return collected, False


def _sync_window(
    config: SyncConfig,
    ch_client: Any,
    http_client: Any,
    run_id: str,
    run_type: str,
    window: SyncWindow,
    posts_lookback: int,
    comments_lookback: int,
    comments_cursor_ts: datetime | None,
) -> tuple[SyncCounters, bool]:
    counters = SyncCounters()
    ingested_at = utc_now()
    window_since_unix = int(window.start.timestamp())
    window_until_unix = int(window.end.timestamp())

    page_payload = _fetch_page_profile_payload(http_client, config)
    counters.rows_extracted += 1
    profile_raw_rows, profile_curated_rows = build_page_profile_rows(
        config.page_id, page_payload, run_id, ingested_at
    )
    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_fb_page_profile",
        [
            "fb_page_id",
            "name",
            "username",
            "category",
            "about",
            "description",
            "link",
            "fan_count",
            "followers_count",
            "verification_status",
            "is_published",
            "overall_star_rating",
            "rating_count",
            #"source_updated_at", breaks in FB v3.3
            "payload_json",
            "run_id",
            "ingested_at",
        ],
        profile_raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_fb_page_profile_current",
        [
            "fb_page_id",
            "name",
            "username",
            "category",
            "about",
            "description",
            "link",
            "fan_count",
            "followers_count",
            "verification_status",
            "is_published",
            "overall_star_rating",
            "rating_count",
            #"source_updated_at", breaks in FB v3.3
            "version_ts",
        ],
        profile_curated_rows,
    )

    post_items, post_source_path = _fetch_posts_for_window(
        http_client=http_client,
        config=config,
        window_since_unix=window_since_unix,
        window_until_unix=window_until_unix,
    )
    post_rows = build_post_rows(
        config.page_id,
        post_items,
        run_id,
        ingested_at,
    )
    counters.rows_extracted += len(post_rows.raw_rows)
    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_fb_page_posts",
        [
            "fb_page_id",
            "fb_post_id",
            "message",
            "story",
            "permalink_url",
            "status_type",
            #"post_type", #this break. FB has deprected this in v3.3
            "full_picture",
            "shares_count",
            "reactions_count",
            "comments_count",
            #"attachments_json", #not needed
            "source_created_at",
            "source_updated_at",
            "payload_json",
            "run_id",
            "ingested_at",
        ],
        post_rows.raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_fb_page_posts_current",
        [
            "fb_page_id",
            "fb_post_id",
            "message",
            "story",
            "permalink_url",
            "status_type",
            #"post_type",
            "full_picture",
            "shares_count",
            "reactions_count",
            "comments_count",
            #"attachments_json", #not needed
            "source_created_at",
            "source_updated_at",
            "version_ts",
        ],
        post_rows.curated_rows,
    )
    print(
        f"[INFO] fetched posts rows={len(post_rows.raw_rows)} "
        f"window={window.start.isoformat()}..{window.end.isoformat()} "
        f"source={post_source_path}"
    )

    probe_seed_limit = max(
        1,
        config.max_post_insight_requests,
        config.comments_post_scan_limit,
    )
    recent_post_ids = get_recent_post_ids(ch_client, config.page_id, probe_seed_limit)
    probe_post_ids = _merge_probe_ids(post_rows.post_ids, recent_post_ids)

    page_insight_items, page_insights_permission_skipped = _collect_page_insights_for_window(
        http_client=http_client,
        config=config,
        since_unix=window_since_unix,
        until_unix=window_until_unix,
    )
    page_insight_raw_rows, page_insight_curated_rows = flatten_insight_rows(
        page_id=config.page_id,
        entity_type="page",
        entity_id=config.page_id,
        insight_items=page_insight_items,
        run_id=run_id,
        ingested_at=ingested_at,
    )
    counters.rows_extracted += len(page_insight_raw_rows)
    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_fb_page_insights",
        [
            "fb_page_id",
            "entity_type",
            "entity_id",
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
        page_insight_raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_fb_page_insights",
        [
            "fb_page_id",
            "entity_type",
            "entity_id",
            "metric",
            "period",
            "end_time",
            "breakdown_key",
            "metric_value_float",
            "metric_value_json",
            "version_ts",
        ],
        page_insight_curated_rows,
    )

    post_insight_items, post_insights_permission_skipped = _collect_post_insights_for_posts(
        http_client=http_client,
        config=config,
        post_ids=probe_post_ids,
    )
    post_insight_raw_rows: list[tuple[Any, ...]] = []
    post_insight_curated_rows: list[tuple[Any, ...]] = []
    for item in post_insight_items:
        post_id = item.get("_post_id")
        if not post_id:
            continue
        item_copy = dict(item)
        item_copy.pop("_post_id", None)
        rows_raw, rows_curated = flatten_insight_rows(
            page_id=config.page_id,
            entity_type="post",
            entity_id=post_id,
            insight_items=[item_copy],
            run_id=run_id,
            ingested_at=ingested_at,
        )
        post_insight_raw_rows.extend(rows_raw)
        post_insight_curated_rows.extend(rows_curated)

    counters.rows_extracted += len(post_insight_raw_rows)
    counters.rows_loaded_raw += insert_rows(
        ch_client,
        "raw_fb_post_insights",
        [
            "fb_page_id",
            "entity_type",
            "entity_id",
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
        post_insight_raw_rows,
    )
    counters.rows_loaded_curated += insert_rows(
        ch_client,
        "curated_fb_post_insights",
        [
            "fb_page_id",
            "entity_type",
            "entity_id",
            "metric",
            "period",
            "end_time",
            "breakdown_key",
            "metric_value_float",
            "metric_value_json",
            "version_ts",
        ],
        post_insight_curated_rows,
    )
    print(
        f"[INFO] page insight rows={len(page_insight_raw_rows)} "
        f"post insight rows={len(post_insight_raw_rows)}"
    )

    comments_permission_skipped = False
    if config.disable_comments:
        print("[INFO] comments disabled by config")
    else:
        comments_since = _resolve_comments_since(
            window=window,
            comments_cursor_ts=comments_cursor_ts,
            comments_lookback_hours=comments_lookback,
            comments_backfill_days=config.comments_backfill_days,
            run_type=run_type,
        )
        comments_since_unix = int(comments_since.timestamp())
        seen_comment_ids: set[str] = set()
        comment_raw_rows: list[tuple[Any, ...]] = []
        comment_curated_rows: list[tuple[Any, ...]] = []

        comment_targets = probe_post_ids[: config.comments_post_scan_limit]
        print(f"[INFO] comment post targets={len(comment_targets)}")
        for post_id in comment_targets:
            try:
                comment_items = _fetch_collection_with_candidates(
                    http_client=http_client,
                    config=config,
                    path=f"/{post_id}/comments",
                    field_candidates=COMMENT_FIELDS_CANDIDATES,
                    limit=config.comments_page_size,
                    since_unix=comments_since_unix,
                    until_unix=window_until_unix,
                )
            except GraphAPIError as exc:
                if is_permission_error(exc):
                    print(f"[WARN] skipping comments stream: {exc}")
                    comments_permission_skipped = True
                    break
                raise

            comment_rows = build_comment_rows_for_post(
                config.page_id,
                post_id,
                comment_items,
                run_id,
                ingested_at,
                seen_comment_ids,
            )
            comment_raw_rows.extend(comment_rows.raw_rows)
            comment_curated_rows.extend(comment_rows.curated_rows)

        counters.rows_extracted += len(comment_raw_rows)
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_fb_post_comments",
            [
                "fb_page_id",
                "fb_post_id",
                "fb_comment_id",
                "parent_comment_id",
                "from_id",
                "from_name",
                "message",
                "like_count",
                "comment_count",
                "is_hidden",
                "source_created_at",
                "source_updated_at",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            comment_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_fb_post_comments_current",
            [
                "fb_page_id",
                "fb_post_id",
                "fb_comment_id",
                "parent_comment_id",
                "from_id",
                "from_name",
                "message",
                "like_count",
                "comment_count",
                "is_hidden",
                "source_created_at",
                "source_updated_at",
                "version_ts",
            ],
            comment_curated_rows,
        )
        print(f"[INFO] fetched comments rows={len(comment_raw_rows)}")

    state_rows = [
        (
            config.page_id,
            POSTS_CURSOR_STREAM,
            window.end.isoformat(),
            window.end,
            posts_lookback,
            run_id,
            "{}",
            utc_now(),
        )
    ]
    if not config.disable_comments and not comments_permission_skipped:
        state_rows.append(
            (
                config.page_id,
                COMMENTS_CURSOR_STREAM,
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

    if page_insights_permission_skipped:
        print("[INFO] page insights permission unavailable; continuing without page insights")
    if post_insights_permission_skipped:
        print("[INFO] post insights permission unavailable; continuing without post insights")

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
        ch_client = _connect_clickhouse(config, clickhouse_connect_module)
        http_client = httpx_module.Client(timeout=config.http_timeout_seconds)

        print(f"[INFO] run_id={run_id}")
        print(f"[INFO] loading stream state for page_id={config.page_id}")
        print(f"[INFO] graph_base={config.graph_base} graph_version={config.graph_version}")

        _, posts_cursor_ts, posts_stored_lookback = load_state(
            ch_client, config.page_id, POSTS_CURSOR_STREAM
        )
        _, comments_cursor_ts, comments_stored_lookback = load_state(
            ch_client, config.page_id, COMMENTS_CURSOR_STREAM
        )

        posts_lookback = (
            config.lookback_hours if config.lookback_hours > 0 else posts_stored_lookback
        )
        comments_lookback = (
            config.comments_lookback_hours
            if config.comments_lookback_hours > 0
            else comments_stored_lookback
        )

        windows, run_type, mode = _plan_post_windows(
            config,
            posts_cursor_ts,
            posts_lookback,
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
            account_id=config.page_id,
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
                account_id=config.page_id,
                stream=POSTS_CURSOR_STREAM,
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
                    account_id=config.page_id,
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
                account_id=config.page_id,
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
                    posts_lookback=posts_lookback,
                    comments_lookback=comments_lookback,
                    comments_cursor_ts=current_comments_cursor_ts,
                )

                counters.rows_extracted += window_counters.rows_extracted
                counters.rows_loaded_raw += window_counters.rows_loaded_raw
                counters.rows_loaded_curated += window_counters.rows_loaded_curated

                complete_stream_window(
                    ch_client,
                    account_id=config.page_id,
                    stream=POSTS_CURSOR_STREAM,
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
                    account_id=config.page_id,
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

            except Exception as exc:  
                error_message = str(exc)[:4000]
                complete_stream_window(
                    ch_client,
                    account_id=config.page_id,
                    stream=POSTS_CURSOR_STREAM,
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
                    account_id=config.page_id,
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
            account_id=config.page_id,
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

    except Exception as exc:  
        finished_at = utc_now()
        error_message = str(exc)[:4000]
        if ch_client is not None:
            try:
                insert_sync_run(
                    ch_client=ch_client,
                    run_id=run_id,
                    account_id=config.page_id,
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
                #traceback.print_exc()
                pass
        print(f"[ERROR] sync failed: {exc}")
        #traceback.print_exc() #sexy debugging
        return 1
    finally:
        if http_client is not None:
            http_client.close()
        if ch_client is not None:
            ch_client.close()
        lock_handle.close()
