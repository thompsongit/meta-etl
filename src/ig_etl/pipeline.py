from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
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
    BUSINESS_DISCOVERY_PROFILE_FIELDS_CANDIDATES,
    CHILD_MEDIA_FIELDS_CANDIDATES,
    COMMENT_REPLY_FIELDS_CANDIDATES,
    CONVERSATION_FIELDS_CANDIDATES,
    HASHTAG_MEDIA_FIELDS_CANDIDATES,
    MENTIONED_MEDIA_FIELDS_CANDIDATES,
    MEDIA_FIELDS_CANDIDATES,
    MEDIA_INSIGHT_CANDIDATES,
    MESSAGE_FIELDS_CANDIDATES,
    STORY_FIELDS_CANDIDATES,
    STREAM_NAME,
    TAG_FIELDS_CANDIDATES,
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
    build_business_discovery_rows,
    build_comment_rows_for_media,
    build_comment_reply_rows,
    build_conversation_rows,
    build_hashtag_lookup_rows,
    build_hashtag_media_rows,
    build_media_rows,
    build_media_children_rows,
    build_mentioned_media_rows,
    build_message_detail_rows,
    build_message_rows,
    build_profile_rows,
    build_story_rows,
    build_tag_rows,
    flatten_insight_rows,
)
from .utils import parse_graph_timestamp, utc_now


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


def _is_skippable_stream_error(exc: GraphAPIError) -> bool:
    if is_permission_error(exc):
        return True
    if exc.code == 100:
        return True
    message = exc.message.lower()
    return any(
        token in message
        for token in (
            "permission",
            "permissions",
            "unsupported",
            "nonexisting field",
            "unknown path",
            "cannot query",
            "does not exist",
        )
    )


def _fetch_collection_with_candidates(
    http_client: Any,
    config: SyncConfig,
    path: str,
    field_candidates: list[str],
    limit: int,
    since_unix: int | None = None,
    until_unix: int | None = None,
    extra_params: dict[str, Any] | None = None,
    timestamp_key: str = "timestamp",
) -> list[dict[str, Any]]:
    params_extra = dict(extra_params or {})
    last_exc: GraphAPIError | None = None
    since_dt = (
        datetime.fromtimestamp(since_unix, tz=timezone.utc)
        if since_unix is not None
        else None
    )
    until_dt = (
        datetime.fromtimestamp(until_unix, tz=timezone.utc)
        if until_unix is not None
        else None
    )

    for fields in field_candidates:
        param_variants: list[dict[str, Any]] = []
        base = {"fields": fields, "limit": limit, **params_extra}
        if since_unix is not None and until_unix is not None:
            param_variants.append({**base, "since": since_unix, "until": until_unix})
        if since_unix is not None:
            param_variants.append({**base, "since": since_unix})
        if until_unix is not None:
            param_variants.append({**base, "until": until_unix})
        param_variants.append(base)

        for params in param_variants:
            try:
                rows = iter_graph_collection(
                    http_client,
                    config.graph_base,
                    config.graph_version,
                    config.graph_token,
                    path,
                    params,
                )
                if since_dt is not None or until_dt is not None:
                    filtered_rows: list[dict[str, Any]] = []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        row_ts = parse_graph_timestamp(row.get(timestamp_key))
                        if row_ts is None:
                            filtered_rows.append(row)
                            continue
                        if since_dt is not None and row_ts < since_dt:
                            continue
                        if until_dt is not None and row_ts > until_dt:
                            continue
                        filtered_rows.append(row)
                    rows = filtered_rows
                return [row for row in rows if isinstance(row, dict)]
            except GraphAPIError as exc:
                last_exc = exc
                if is_permission_error(exc):
                    raise
                message = exc.message.lower()
                if exc.code == 100 and (
                    "field" in message
                    or "nonexisting field" in message
                    or "since" in message
                    or "until" in message
                    or "parameter" in message
                    or "unsupported" in message
                ):
                    continue
                raise

    if last_exc is not None:
        raise last_exc
    return []


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
    media_ids_to_scan: list[str] = []
    seen_comment_ids: set[str] = set()

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

    if config.enable_extended_streams:
        window_since_unix = int(window.start.timestamp())
        window_until_unix = int(window.end.timestamp())

        child_raw_rows: list[tuple[Any, ...]] = []
        child_curated_rows: list[tuple[Any, ...]] = []
        child_probe_count = 0
        child_parent_ids = list(dict.fromkeys(media_rows.media_ids + media_ids_for_insights))
        for media_id in child_parent_ids:
            try:
                child_items = _fetch_collection_with_candidates(
                    http_client=http_client,
                    config=config,
                    path=f"/{media_id}/children",
                    field_candidates=CHILD_MEDIA_FIELDS_CANDIDATES,
                    limit=config.media_page_size,
                    timestamp_key="timestamp",
                )
            except GraphAPIError as exc:
                if _is_skippable_stream_error(exc):
                    print(f"[WARN] skipping media children for media_id={media_id}: {exc}")
                    continue
                raise

            child_probe_count += 1
            counters.rows_extracted += len(child_items)
            raw_rows, curated_rows = build_media_children_rows(
                config.ig_user_id,
                media_id,
                child_items,
                run_id,
                ingested_at,
            )
            child_raw_rows.extend(raw_rows)
            child_curated_rows.extend(curated_rows)
        print(f"[INFO] media children probes={child_probe_count}")

        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_media_children",
            [
                "ig_user_id",
                "parent_media_id",
                "child_media_id",
                "media_type",
                "media_product_type",
                "permalink",
                "media_url",
                "thumbnail_url",
                "source_timestamp",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            child_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_media_children_current",
            [
                "ig_user_id",
                "parent_media_id",
                "child_media_id",
                "media_type",
                "media_product_type",
                "permalink",
                "media_url",
                "thumbnail_url",
                "source_timestamp",
                "version_ts",
            ],
            child_curated_rows,
        )

        try:
            story_items = _fetch_collection_with_candidates(
                http_client=http_client,
                config=config,
                path=f"/{config.ig_user_id}/stories",
                field_candidates=STORY_FIELDS_CANDIDATES,
                limit=config.media_page_size,
                since_unix=window_since_unix,
                until_unix=window_until_unix,
                timestamp_key="timestamp",
            )
        except GraphAPIError as exc:
            if _is_skippable_stream_error(exc):
                print(f"[WARN] skipping stories stream: {exc}")
                story_items = []
            else:
                raise
        counters.rows_extracted += len(story_items)
        story_raw_rows, story_curated_rows = build_story_rows(
            config.ig_user_id,
            story_items,
            run_id,
            ingested_at,
        )
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_stories",
            [
                "ig_user_id",
                "ig_story_id",
                "media_type",
                "media_product_type",
                "permalink",
                "media_url",
                "thumbnail_url",
                "source_timestamp",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            story_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_stories_current",
            [
                "ig_user_id",
                "ig_story_id",
                "media_type",
                "media_product_type",
                "permalink",
                "media_url",
                "thumbnail_url",
                "source_timestamp",
                "version_ts",
            ],
            story_curated_rows,
        )
        print(f"[INFO] stories fetched={len(story_items)}")

        try:
            tag_items = _fetch_collection_with_candidates(
                http_client=http_client,
                config=config,
                path=f"/{config.ig_user_id}/tags",
                field_candidates=TAG_FIELDS_CANDIDATES,
                limit=config.media_page_size,
                since_unix=window_since_unix,
                until_unix=window_until_unix,
                timestamp_key="timestamp",
            )
        except GraphAPIError as exc:
            if _is_skippable_stream_error(exc):
                print(f"[WARN] skipping tags stream: {exc}")
                tag_items = []
            else:
                raise
        counters.rows_extracted += len(tag_items)
        tag_raw_rows, tag_curated_rows = build_tag_rows(
            config.ig_user_id,
            tag_items,
            run_id,
            ingested_at,
        )
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_user_tags",
            [
                "ig_user_id",
                "tagged_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            tag_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_user_tags_current",
            [
                "ig_user_id",
                "tagged_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "version_ts",
            ],
            tag_curated_rows,
        )
        print(f"[INFO] tags fetched={len(tag_items)}")

        try:
            mentioned_items = _fetch_collection_with_candidates(
                http_client=http_client,
                config=config,
                path=f"/{config.ig_user_id}/mentioned_media",
                field_candidates=MENTIONED_MEDIA_FIELDS_CANDIDATES,
                limit=config.media_page_size,
                since_unix=window_since_unix,
                until_unix=window_until_unix,
                timestamp_key="timestamp",
            )
        except GraphAPIError as exc:
            if _is_skippable_stream_error(exc):
                print(f"[WARN] skipping mentioned_media stream: {exc}")
                mentioned_items = []
            else:
                raise
        counters.rows_extracted += len(mentioned_items)
        mentioned_raw_rows, mentioned_curated_rows = build_mentioned_media_rows(
            config.ig_user_id,
            mentioned_items,
            run_id,
            ingested_at,
        )
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_mentioned_media",
            [
                "ig_user_id",
                "mentioned_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            mentioned_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_mentioned_media_current",
            [
                "ig_user_id",
                "mentioned_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "version_ts",
            ],
            mentioned_curated_rows,
        )
        print(f"[INFO] mentioned_media fetched={len(mentioned_items)}")

        reply_raw_rows: list[tuple[Any, ...]] = []
        reply_curated_rows: list[tuple[Any, ...]] = []
        if config.disable_comments:
            print("[INFO] comment replies skipped (comments disabled)")
        elif comments_permission_skipped:
            print("[INFO] comment replies skipped (comments permission unavailable)")
        elif not seen_comment_ids:
            print("[INFO] comment replies skipped (no comment ids in window)")
        else:
            seen_reply_ids: set[str] = set()
            reply_probe_count = 0
            comment_media_pairs = [
                (row[2], row[1]) for row in comment_raw_rows if len(row) > 3 and row[2] and row[1]
            ]
            for comment_id, media_id in comment_media_pairs:
                try:
                    reply_items = _fetch_collection_with_candidates(
                        http_client=http_client,
                        config=config,
                        path=f"/{comment_id}/replies",
                        field_candidates=COMMENT_REPLY_FIELDS_CANDIDATES,
                        limit=config.comments_page_size,
                        since_unix=comments_since_unix,
                        until_unix=comments_until_unix,
                        timestamp_key="timestamp",
                    )
                except GraphAPIError as exc:
                    if _is_skippable_stream_error(exc):
                        print(f"[WARN] skipping comment replies for comment_id={comment_id}: {exc}")
                        continue
                    raise

                filtered_replies: list[dict[str, Any]] = []
                for reply in reply_items:
                    reply_id = reply.get("id")
                    if not reply_id or reply_id in seen_reply_ids:
                        continue
                    seen_reply_ids.add(reply_id)
                    filtered_replies.append(reply)

                reply_probe_count += 1
                counters.rows_extracted += len(filtered_replies)
                raw_rows, curated_rows = build_comment_reply_rows(
                    config.ig_user_id,
                    media_id,
                    comment_id,
                    filtered_replies,
                    run_id,
                    ingested_at,
                )
                reply_raw_rows.extend(raw_rows)
                reply_curated_rows.extend(curated_rows)
            print(f"[INFO] comment reply probes={reply_probe_count}")

        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_comment_replies",
            [
                "ig_user_id",
                "ig_media_id",
                "parent_comment_id",
                "ig_reply_id",
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
            reply_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_comment_replies_current",
            [
                "ig_user_id",
                "ig_media_id",
                "parent_comment_id",
                "ig_reply_id",
                "text",
                "username",
                "like_count",
                "hidden",
                "source_timestamp",
                "source_updated_at",
                "version_ts",
            ],
            reply_curated_rows,
        )

        hashtag_lookup_raw_rows: list[tuple[Any, ...]] = []
        hashtag_lookup_curated_rows: list[tuple[Any, ...]] = []
        top_hashtag_raw_rows: list[tuple[Any, ...]] = []
        top_hashtag_curated_rows: list[tuple[Any, ...]] = []
        recent_hashtag_raw_rows: list[tuple[Any, ...]] = []
        recent_hashtag_curated_rows: list[tuple[Any, ...]] = []
        hashtag_ids: list[str] = []
        for hashtag_name in config.hashtag_names:
            try:
                hashtag_lookup_payload = graph_get_json(
                    http_client,
                    config.graph_base,
                    config.graph_version,
                    config.graph_token,
                    "/ig_hashtag_search",
                    params={
                        "user_id": config.ig_user_id,
                        "q": hashtag_name,
                    },
                )
            except GraphAPIError as exc:
                if _is_skippable_stream_error(exc):
                    print(f"[WARN] skipping hashtag lookup name={hashtag_name}: {exc}")
                    continue
                raise

            hashtag_items = hashtag_lookup_payload.get("data", [])
            if not isinstance(hashtag_items, list):
                hashtag_items = []
            counters.rows_extracted += len(hashtag_items)
            raw_rows, curated_rows, discovered_ids = build_hashtag_lookup_rows(
                config.ig_user_id,
                hashtag_name,
                hashtag_items,
                run_id,
                ingested_at,
            )
            hashtag_lookup_raw_rows.extend(raw_rows)
            hashtag_lookup_curated_rows.extend(curated_rows)
            hashtag_ids.extend(discovered_ids)

        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_hashtag_lookup",
            [
                "ig_user_id",
                "hashtag_name",
                "ig_hashtag_id",
                "source_fetched_at",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            hashtag_lookup_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_hashtag_lookup_current",
            [
                "ig_user_id",
                "hashtag_name",
                "ig_hashtag_id",
                "source_fetched_at",
                "version_ts",
            ],
            hashtag_lookup_curated_rows,
        )

        hashtag_probe_count = 0
        for hashtag_id in dict.fromkeys(hashtag_ids):
            try:
                top_media_items = _fetch_collection_with_candidates(
                    http_client=http_client,
                    config=config,
                    path=f"/{hashtag_id}/top_media",
                    field_candidates=HASHTAG_MEDIA_FIELDS_CANDIDATES,
                    limit=config.media_page_size,
                    extra_params={"user_id": config.ig_user_id},
                    timestamp_key="timestamp",
                )
            except GraphAPIError as exc:
                if _is_skippable_stream_error(exc):
                    print(f"[WARN] skipping hashtag top_media hashtag_id={hashtag_id}: {exc}")
                    top_media_items = []
                else:
                    raise
            counters.rows_extracted += len(top_media_items)
            raw_rows, curated_rows = build_hashtag_media_rows(
                config.ig_user_id,
                hashtag_id,
                top_media_items,
                run_id,
                ingested_at,
            )
            top_hashtag_raw_rows.extend(raw_rows)
            top_hashtag_curated_rows.extend(curated_rows)

            try:
                recent_media_items = _fetch_collection_with_candidates(
                    http_client=http_client,
                    config=config,
                    path=f"/{hashtag_id}/recent_media",
                    field_candidates=HASHTAG_MEDIA_FIELDS_CANDIDATES,
                    limit=config.media_page_size,
                    since_unix=window_since_unix,
                    until_unix=window_until_unix,
                    extra_params={"user_id": config.ig_user_id},
                    timestamp_key="timestamp",
                )
            except GraphAPIError as exc:
                if _is_skippable_stream_error(exc):
                    print(f"[WARN] skipping hashtag recent_media hashtag_id={hashtag_id}: {exc}")
                    recent_media_items = []
                else:
                    raise
            counters.rows_extracted += len(recent_media_items)
            raw_rows, curated_rows = build_hashtag_media_rows(
                config.ig_user_id,
                hashtag_id,
                recent_media_items,
                run_id,
                ingested_at,
            )
            recent_hashtag_raw_rows.extend(raw_rows)
            recent_hashtag_curated_rows.extend(curated_rows)
            hashtag_probe_count += 1
        print(f"[INFO] hashtag media probes={hashtag_probe_count}")

        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_hashtag_top_media",
            [
                "ig_user_id",
                "ig_hashtag_id",
                "ig_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            top_hashtag_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_hashtag_top_media_current",
            [
                "ig_user_id",
                "ig_hashtag_id",
                "ig_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "version_ts",
            ],
            top_hashtag_curated_rows,
        )
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_hashtag_recent_media",
            [
                "ig_user_id",
                "ig_hashtag_id",
                "ig_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            recent_hashtag_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_hashtag_recent_media_current",
            [
                "ig_user_id",
                "ig_hashtag_id",
                "ig_media_id",
                "media_type",
                "permalink",
                "caption",
                "source_timestamp",
                "version_ts",
            ],
            recent_hashtag_curated_rows,
        )

        bd_profile_raw_rows: list[tuple[Any, ...]] = []
        bd_profile_curated_rows: list[tuple[Any, ...]] = []
        bd_media_raw_rows: list[tuple[Any, ...]] = []
        bd_media_curated_rows: list[tuple[Any, ...]] = []
        bd_probe_count = 0
        for username in config.business_discovery_usernames:
            discovered_payload: dict[str, Any] | None = None
            last_exc: GraphAPIError | None = None
            for fields in BUSINESS_DISCOVERY_PROFILE_FIELDS_CANDIDATES:
                query_field = f"business_discovery.username({username}){{{fields}}}"
                try:
                    response_payload = graph_get_json(
                        http_client,
                        config.graph_base,
                        config.graph_version,
                        config.graph_token,
                        f"/{config.ig_user_id}",
                        params={"fields": query_field},
                    )
                except GraphAPIError as exc:
                    last_exc = exc
                    if is_permission_error(exc):
                        break
                    if exc.code == 100:
                        continue
                    raise

                candidate_payload = response_payload.get("business_discovery")
                if isinstance(candidate_payload, dict):
                    discovered_payload = candidate_payload
                    break
                discovered_payload = {}
                break

            if discovered_payload is None:
                if last_exc is not None and _is_skippable_stream_error(last_exc):
                    print(f"[WARN] skipping business discovery username={username}: {last_exc}")
                    continue
                if last_exc is not None:
                    raise last_exc
                continue

            if not discovered_payload.get("id"):
                print(f"[INFO] business discovery returned no account for username={username}")
                continue

            media_items = discovered_payload.get("media", {}).get("data", [])
            media_count = len(media_items) if isinstance(media_items, list) else 0
            counters.rows_extracted += 1 + media_count
            profile_raw_rows, profile_curated_rows, media_raw_rows, media_curated_rows = (
                build_business_discovery_rows(
                    config.ig_user_id,
                    discovered_payload,
                    run_id,
                    ingested_at,
                )
            )
            bd_profile_raw_rows.extend(profile_raw_rows)
            bd_profile_curated_rows.extend(profile_curated_rows)
            bd_media_raw_rows.extend(media_raw_rows)
            bd_media_curated_rows.extend(media_curated_rows)
            bd_probe_count += 1
        print(f"[INFO] business discovery probes={bd_probe_count}")

        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_business_discovery_profile",
            [
                "source_ig_user_id",
                "discovered_ig_user_id",
                "discovered_username",
                "discovered_name",
                "biography",
                "website",
                "followers_count",
                "follows_count",
                "media_count",
                "source_fetched_at",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            bd_profile_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_business_discovery_profile_current",
            [
                "source_ig_user_id",
                "discovered_ig_user_id",
                "discovered_username",
                "discovered_name",
                "biography",
                "website",
                "followers_count",
                "follows_count",
                "media_count",
                "source_fetched_at",
                "version_ts",
            ],
            bd_profile_curated_rows,
        )
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_business_discovery_media",
            [
                "source_ig_user_id",
                "discovered_ig_user_id",
                "ig_media_id",
                "media_type",
                "media_product_type",
                "permalink",
                "caption",
                "source_timestamp",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            bd_media_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_business_discovery_media_current",
            [
                "source_ig_user_id",
                "discovered_ig_user_id",
                "ig_media_id",
                "media_type",
                "media_product_type",
                "permalink",
                "caption",
                "source_timestamp",
                "version_ts",
            ],
            bd_media_curated_rows,
        )

        conversation_items: list[dict[str, Any]] = []
        try:
            conversation_items = _fetch_collection_with_candidates(
                http_client=http_client,
                config=config,
                path=f"/{config.ig_user_id}/conversations",
                field_candidates=CONVERSATION_FIELDS_CANDIDATES,
                limit=config.messages_page_size,
                since_unix=window_since_unix,
                until_unix=window_until_unix,
                timestamp_key="updated_time",
            )
        except GraphAPIError as exc:
            if _is_skippable_stream_error(exc):
                print(f"[WARN] skipping conversations stream: {exc}")
                conversation_items = []
            else:
                raise

        counters.rows_extracted += len(conversation_items)
        conversation_raw_rows, conversation_curated_rows = build_conversation_rows(
            config.ig_user_id,
            conversation_items,
            run_id,
            ingested_at,
        )
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_conversations",
            [
                "ig_user_id",
                "conversation_id",
                "updated_time",
                "participants_json",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            conversation_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_conversations_current",
            [
                "ig_user_id",
                "conversation_id",
                "updated_time",
                "participants_json",
                "version_ts",
            ],
            conversation_curated_rows,
        )

        message_raw_rows: list[tuple[Any, ...]] = []
        message_curated_rows: list[tuple[Any, ...]] = []
        message_detail_raw_rows: list[tuple[Any, ...]] = []
        message_detail_curated_rows: list[tuple[Any, ...]] = []
        message_probe_count = 0
        message_detail_probe_count = 0
        for conversation in conversation_items:
            conversation_id = conversation.get("id")
            if not conversation_id:
                continue
            try:
                message_items = _fetch_collection_with_candidates(
                    http_client=http_client,
                    config=config,
                    path=f"/{conversation_id}/messages",
                    field_candidates=MESSAGE_FIELDS_CANDIDATES,
                    limit=config.messages_page_size,
                    since_unix=window_since_unix,
                    until_unix=window_until_unix,
                    timestamp_key="created_time",
                )
            except GraphAPIError as exc:
                if _is_skippable_stream_error(exc):
                    print(f"[WARN] skipping messages for conversation_id={conversation_id}: {exc}")
                    continue
                raise

            counters.rows_extracted += len(message_items)
            raw_rows, curated_rows, message_ids = build_message_rows(
                config.ig_user_id,
                conversation_id,
                message_items,
                run_id,
                ingested_at,
            )
            message_raw_rows.extend(raw_rows)
            message_curated_rows.extend(curated_rows)
            message_probe_count += 1

            for message_id in message_ids:
                try:
                    message_payload = graph_get_json(
                        http_client,
                        config.graph_base,
                        config.graph_version,
                        config.graph_token,
                        f"/{message_id}",
                        params={"fields": "id,created_time,conversation"},
                    )
                except GraphAPIError as exc:
                    if _is_skippable_stream_error(exc):
                        continue
                    raise
                counters.rows_extracted += 1
                raw_detail_rows, curated_detail_rows = build_message_detail_rows(
                    config.ig_user_id,
                    message_payload,
                    run_id,
                    ingested_at,
                )
                message_detail_raw_rows.extend(raw_detail_rows)
                message_detail_curated_rows.extend(curated_detail_rows)
                message_detail_probe_count += 1

        print(
            f"[INFO] conversation probes={len(conversation_items)} "
            f"message probes={message_probe_count} message detail probes={message_detail_probe_count}"
        )

        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_messages",
            [
                "ig_user_id",
                "conversation_id",
                "message_id",
                "from_id",
                "to_ids_json",
                "text",
                "created_time",
                "is_echo",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            message_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_messages_current",
            [
                "ig_user_id",
                "conversation_id",
                "message_id",
                "from_id",
                "to_ids_json",
                "text",
                "created_time",
                "is_echo",
                "version_ts",
            ],
            message_curated_rows,
        )
        counters.rows_loaded_raw += insert_rows(
            ch_client,
            "raw_ig_message_detail",
            [
                "ig_user_id",
                "message_id",
                "conversation_id",
                "created_time",
                "payload_json",
                "run_id",
                "ingested_at",
            ],
            message_detail_raw_rows,
        )
        counters.rows_loaded_curated += insert_rows(
            ch_client,
            "curated_ig_message_detail_current",
            [
                "ig_user_id",
                "message_id",
                "conversation_id",
                "created_time",
                "version_ts",
            ],
            message_detail_curated_rows,
        )
        print("[INFO] webhook events stream not polled (webhook push-only)")
    else:
        print("[INFO] extended streams disabled by config")

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
        ch_client = _connect_clickhouse(config, clickhouse_connect_module)
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
