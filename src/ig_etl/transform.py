from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import CommentRows, MediaRows
from .utils import (
    as_nullable_float64,
    as_nullable_uint8,
    as_nullable_uint64,
    json_compact,
    parse_graph_timestamp,
)


def build_profile_rows(
    ig_user_id: str,
    payload: dict[str, Any],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    legacy_instagram_user_id = payload.get("legacy_instagram_user_id") or payload.get(
        "ig_id"
    )
    raw_rows = [
        (
            ig_user_id,
            legacy_instagram_user_id,
            payload.get("username"),
            payload.get("name"),
            payload.get("biography"),
            payload.get("website"),
            payload.get("profile_picture_url"),
            as_nullable_uint64(payload.get("followers_count")),
            as_nullable_uint64(payload.get("follows_count")),
            as_nullable_uint64(payload.get("media_count")),
            ingested_at,
            json_compact(payload),
            run_id,
            ingested_at,
        )
    ]
    curated_rows = [
        (
            ig_user_id,
            legacy_instagram_user_id,
            payload.get("username"),
            payload.get("name"),
            payload.get("biography"),
            payload.get("website"),
            payload.get("profile_picture_url"),
            as_nullable_uint64(payload.get("followers_count")),
            as_nullable_uint64(payload.get("follows_count")),
            as_nullable_uint64(payload.get("media_count")),
            ingested_at,
            ingested_at,
        )
    ]
    return raw_rows, curated_rows


def build_media_rows(
    ig_user_id: str,
    media_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> MediaRows:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    media_ids: list[str] = []
    media_max_ts: datetime | None = None

    for media in media_items:
        media_id = media.get("id")
        if not media_id:
            continue
        media_ts = parse_graph_timestamp(media.get("timestamp"))
        if media_ts and (media_max_ts is None or media_ts > media_max_ts):
            media_max_ts = media_ts
        media_ids.append(media_id)

        raw_rows.append(
            (
                ig_user_id,
                media_id,
                media.get("media_type"),
                media.get("media_product_type"),
                media.get("permalink"),
                media.get("media_url"),
                media.get("thumbnail_url"),
                media.get("caption"),
                media.get("username"),
                as_nullable_uint8(media.get("is_comment_enabled")),
                as_nullable_uint64(media.get("like_count")),
                as_nullable_uint64(media.get("comments_count")),
                media_ts,
                media_ts,
                json_compact(media),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                media_id,
                media.get("media_type"),
                media.get("media_product_type"),
                media.get("permalink"),
                media.get("media_url"),
                media.get("thumbnail_url"),
                media.get("caption"),
                media.get("username"),
                as_nullable_uint8(media.get("is_comment_enabled")),
                as_nullable_uint64(media.get("like_count")),
                as_nullable_uint64(media.get("comments_count")),
                media_ts,
                media_ts,
                ingested_at,
            )
        )

    return MediaRows(
        raw_rows=raw_rows,
        curated_rows=curated_rows,
        media_ids=media_ids,
        max_timestamp=media_max_ts,
    )


def flatten_insight_rows(
    ig_user_id: str,
    ig_media_id: str | None,
    insight_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []

    for metric_obj in insight_items:
        metric = metric_obj.get("name")
        if not metric:
            continue
        period = metric_obj.get("period", "lifetime")
        title = metric_obj.get("title")
        description = metric_obj.get("description")

        values = metric_obj.get("values")
        if not values:
            values = [{"value": metric_obj.get("value"), "end_time": metric_obj.get("end_time")}]
        if not isinstance(values, list):
            values = [{"value": values, "end_time": None}]

        for value_obj in values:
            value = value_obj.get("value")
            end_time = parse_graph_timestamp(value_obj.get("end_time"))
            breakdown_key = None
            if "breakdown" in value_obj:
                breakdown_key = json_compact(value_obj.get("breakdown"))

            metric_value_float = as_nullable_float64(value)
            metric_value_json = json_compact(value)
            source_updated_at = end_time or ingested_at

            if ig_media_id is not None:
                raw_rows.append(
                    (
                        ig_user_id,
                        ig_media_id,
                        metric,
                        period,
                        end_time,
                        breakdown_key,
                        metric_value_float,
                        metric_value_json,
                        title,
                        description,
                        source_updated_at,
                        json_compact(metric_obj),
                        run_id,
                        ingested_at,
                    )
                )
                curated_rows.append(
                    (
                        ig_user_id,
                        ig_media_id,
                        metric,
                        period,
                        end_time,
                        breakdown_key,
                        metric_value_float,
                        metric_value_json,
                        ingested_at,
                    )
                )
            else:
                raw_rows.append(
                    (
                        ig_user_id,
                        metric,
                        period,
                        end_time,
                        breakdown_key,
                        metric_value_float,
                        metric_value_json,
                        title,
                        description,
                        source_updated_at,
                        json_compact(metric_obj),
                        run_id,
                        ingested_at,
                    )
                )
                curated_rows.append(
                    (
                        ig_user_id,
                        metric,
                        period,
                        end_time,
                        breakdown_key,
                        metric_value_float,
                        metric_value_json,
                        ingested_at,
                    )
                )

    return raw_rows, curated_rows


def build_comment_rows_for_media(
    ig_user_id: str,
    media_id: str,
    comment_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
    seen_comment_ids: set[str],
) -> CommentRows:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    comments_max_ts: datetime | None = None

    for comment in comment_items:
        comment_id = comment.get("id")
        if not comment_id or comment_id in seen_comment_ids:
            continue
        seen_comment_ids.add(comment_id)

        comment_ts = parse_graph_timestamp(comment.get("timestamp"))
        if comment_ts and (comments_max_ts is None or comment_ts > comments_max_ts):
            comments_max_ts = comment_ts

        parent_comment_id = comment.get("parent_id")

        raw_rows.append(
            (
                ig_user_id,
                media_id,
                comment_id,
                parent_comment_id,
                comment.get("text"),
                comment.get("username"),
                as_nullable_uint64(comment.get("like_count")),
                as_nullable_uint8(comment.get("hidden")),
                comment_ts,
                comment_ts,
                json_compact(comment),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                comment_id,
                ig_user_id,
                media_id,
                parent_comment_id,
                comment.get("text"),
                comment.get("username"),
                as_nullable_uint64(comment.get("like_count")),
                as_nullable_uint8(comment.get("hidden")),
                comment_ts,
                comment_ts,
                ingested_at,
            )
        )

    return CommentRows(
        raw_rows=raw_rows,
        curated_rows=curated_rows,
        max_timestamp=comments_max_ts,
    )
