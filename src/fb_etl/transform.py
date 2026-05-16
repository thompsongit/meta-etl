from __future__ import annotations

from datetime import datetime
from typing import Any

from .models import CommentRows, PostRows
from .utils import (
    as_nullable_float64,
    as_nullable_uint8,
    as_nullable_uint64,
    json_compact,
    parse_graph_timestamp,
)


def build_page_profile_rows(
    page_id: str,
    payload: dict[str, Any],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    #source_updated_at = parse_graph_timestamp(payload.get("updated_time"))
    raw_rows = [
        (
            page_id,
            payload.get("name"),
            payload.get("username"),
            payload.get("category"),
            payload.get("about"),
            payload.get("description"),
            payload.get("link"),
            as_nullable_uint64(payload.get("fan_count")),
            as_nullable_uint64(payload.get("followers_count")),
            payload.get("verification_status"),
            as_nullable_uint8(payload.get("is_published")),
            as_nullable_float64(payload.get("overall_star_rating")),
            as_nullable_uint64(payload.get("rating_count")),
            #source_updated_at,
            json_compact(payload),
            run_id,
            ingested_at,
        )
    ]
    curated_rows = [
        (
            page_id,
            payload.get("name"),
            payload.get("username"),
            payload.get("category"),
            payload.get("about"),
            payload.get("description"),
            payload.get("link"),
            as_nullable_uint64(payload.get("fan_count")),
            as_nullable_uint64(payload.get("followers_count")),
            payload.get("verification_status"),
            as_nullable_uint8(payload.get("is_published")),
            as_nullable_float64(payload.get("overall_star_rating")),
            as_nullable_uint64(payload.get("rating_count")),
            #source_updated_at,
            ingested_at,
        )
    ]
    return raw_rows, curated_rows


def _nested_total_count(payload: dict[str, Any], key: str) -> int | None:
    root = payload.get(key)
    if not isinstance(root, dict):
        return None
    summary = root.get("summary")
    if not isinstance(summary, dict):
        return None
    return as_nullable_uint64(summary.get("total_count"))


def build_post_rows(
    page_id: str,
    post_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> PostRows:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    post_ids: list[str] = []
    posts_max_ts: datetime | None = None

    for post in post_items:
        post_id = post.get("id")
        if not post_id:
            continue
        created_at = parse_graph_timestamp(post.get("created_time"))
        updated_at = parse_graph_timestamp(post.get("updated_time")) or created_at
        if updated_at and (posts_max_ts is None or updated_at > posts_max_ts):
            posts_max_ts = updated_at
        post_ids.append(post_id)

        shares_payload = post.get("shares")
        shares_count = (
            as_nullable_uint64(shares_payload.get("count"))
            if isinstance(shares_payload, dict)
            else None
        )
        reactions_count = _nested_total_count(post, "reactions")
        comments_count = _nested_total_count(post, "comments")
        #attachments_json = json_compact(post.get("attachments", {}))

        raw_rows.append(
            (
                page_id,
                post_id,
                post.get("message"),
                post.get("story"),
                post.get("permalink_url"),
                post.get("status_type"),
                #post.get("type"),
                post.get("full_picture"),
                shares_count,
                reactions_count,
                comments_count,
                #attachments_json,
                created_at,
                updated_at,
                json_compact(post),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                page_id,
                post_id,
                post.get("message"),
                post.get("story"),
                post.get("permalink_url"),
                post.get("status_type"),
                #post.get("type"),
                post.get("full_picture"),
                shares_count,
                reactions_count,
                comments_count,
                #attachments_json,
                created_at,
                updated_at,
                ingested_at,
            )
        )

    return PostRows(
        raw_rows=raw_rows,
        curated_rows=curated_rows,
        post_ids=post_ids,
        max_timestamp=posts_max_ts,
    )


def build_comment_rows_for_post(
    page_id: str,
    post_id: str,
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

        created_at = parse_graph_timestamp(comment.get("created_time"))
        if created_at and (comments_max_ts is None or created_at > comments_max_ts):
            comments_max_ts = created_at

        author_payload = comment.get("from")
        author_id = author_payload.get("id") if isinstance(author_payload, dict) else None
        author_name = author_payload.get("name") if isinstance(author_payload, dict) else None

        parent_payload = comment.get("parent")
        parent_comment_id = parent_payload.get("id") if isinstance(parent_payload, dict) else None

        raw_rows.append(
            (
                page_id,
                post_id,
                comment_id,
                parent_comment_id,
                author_id,
                author_name,
                comment.get("message"),
                as_nullable_uint64(comment.get("like_count")),
                as_nullable_uint64(comment.get("comment_count")),
                as_nullable_uint8(comment.get("is_hidden")),
                created_at,
                created_at,
                json_compact(comment),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                page_id,
                post_id,
                comment_id,
                parent_comment_id,
                author_id,
                author_name,
                comment.get("message"),
                as_nullable_uint64(comment.get("like_count")),
                as_nullable_uint64(comment.get("comment_count")),
                as_nullable_uint8(comment.get("is_hidden")),
                created_at,
                created_at,
                ingested_at,
            )
        )

    return CommentRows(
        raw_rows=raw_rows,
        curated_rows=curated_rows,
        max_timestamp=comments_max_ts,
    )


def flatten_insight_rows(
    page_id: str,
    entity_type: str,
    entity_id: str | None,
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
            source_updated_at = end_time

            raw_rows.append(
                (
                    page_id,
                    entity_type,
                    entity_id,
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
                    page_id,
                    entity_type,
                    entity_id,
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

