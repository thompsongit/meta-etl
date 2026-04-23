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
    source_updated_at = parse_graph_timestamp(payload.get("updated_time"))
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
            source_updated_at,
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
            source_updated_at,
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
            source_updated_at = end_time

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


def build_media_children_rows(
    ig_user_id: str,
    parent_media_id: str,
    child_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    for child in child_items:
        child_media_id = child.get("id")
        if not child_media_id:
            continue
        source_ts = parse_graph_timestamp(child.get("timestamp"))
        raw_rows.append(
            (
                ig_user_id,
                parent_media_id,
                child_media_id,
                child.get("media_type"),
                child.get("media_product_type"),
                child.get("permalink"),
                child.get("media_url"),
                child.get("thumbnail_url"),
                source_ts,
                json_compact(child),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                parent_media_id,
                child_media_id,
                child.get("media_type"),
                child.get("media_product_type"),
                child.get("permalink"),
                child.get("media_url"),
                child.get("thumbnail_url"),
                source_ts,
                ingested_at,
            )
        )
    return raw_rows, curated_rows


def build_story_rows(
    ig_user_id: str,
    story_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    for story in story_items:
        story_id = story.get("id")
        if not story_id:
            continue
        source_ts = parse_graph_timestamp(story.get("timestamp"))
        raw_rows.append(
            (
                ig_user_id,
                story_id,
                story.get("media_type"),
                story.get("media_product_type"),
                story.get("permalink"),
                story.get("media_url"),
                story.get("thumbnail_url"),
                source_ts,
                json_compact(story),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                story_id,
                story.get("media_type"),
                story.get("media_product_type"),
                story.get("permalink"),
                story.get("media_url"),
                story.get("thumbnail_url"),
                source_ts,
                ingested_at,
            )
        )
    return raw_rows, curated_rows


def build_tag_rows(
    ig_user_id: str,
    items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    for item in items:
        media_id = item.get("id")
        if not media_id:
            continue
        source_ts = parse_graph_timestamp(item.get("timestamp"))
        raw_rows.append(
            (
                ig_user_id,
                media_id,
                item.get("media_type"),
                item.get("permalink"),
                item.get("caption"),
                source_ts,
                json_compact(item),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                media_id,
                item.get("media_type"),
                item.get("permalink"),
                item.get("caption"),
                source_ts,
                ingested_at,
            )
        )
    return raw_rows, curated_rows


def build_mentioned_media_rows(
    ig_user_id: str,
    items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    return build_tag_rows(ig_user_id, items, run_id, ingested_at)


def build_comment_reply_rows(
    ig_user_id: str,
    media_id: str,
    parent_comment_id: str,
    reply_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    for reply in reply_items:
        reply_id = reply.get("id")
        if not reply_id:
            continue
        source_ts = parse_graph_timestamp(reply.get("timestamp"))
        raw_rows.append(
            (
                ig_user_id,
                media_id,
                parent_comment_id,
                reply_id,
                reply.get("text"),
                reply.get("username"),
                as_nullable_uint64(reply.get("like_count")),
                as_nullable_uint8(reply.get("hidden")),
                source_ts,
                source_ts,
                json_compact(reply),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                media_id,
                parent_comment_id,
                reply_id,
                reply.get("text"),
                reply.get("username"),
                as_nullable_uint64(reply.get("like_count")),
                as_nullable_uint8(reply.get("hidden")),
                source_ts,
                source_ts,
                ingested_at,
            )
        )
    return raw_rows, curated_rows


def build_hashtag_lookup_rows(
    ig_user_id: str,
    hashtag_name: str,
    hashtag_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[str]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    hashtag_ids: list[str] = []
    for item in hashtag_items:
        hashtag_id = item.get("id")
        if not hashtag_id:
            continue
        hashtag_ids.append(hashtag_id)
        raw_rows.append(
            (
                ig_user_id,
                hashtag_name,
                hashtag_id,
                ingested_at,
                json_compact(item),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                hashtag_name,
                hashtag_id,
                ingested_at,
                ingested_at,
            )
        )
    return raw_rows, curated_rows, hashtag_ids


def build_hashtag_media_rows(
    ig_user_id: str,
    hashtag_id: str,
    media_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    for media in media_items:
        media_id = media.get("id")
        if not media_id:
            continue
        source_ts = parse_graph_timestamp(media.get("timestamp"))
        raw_rows.append(
            (
                ig_user_id,
                hashtag_id,
                media_id,
                media.get("media_type"),
                media.get("permalink"),
                media.get("caption"),
                source_ts,
                json_compact(media),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                hashtag_id,
                media_id,
                media.get("media_type"),
                media.get("permalink"),
                media.get("caption"),
                source_ts,
                ingested_at,
            )
        )
    return raw_rows, curated_rows


def build_business_discovery_rows(
    source_ig_user_id: str,
    discovered_payload: dict[str, Any],
    run_id: str,
    ingested_at: datetime,
) -> tuple[
    list[tuple[Any, ...]],
    list[tuple[Any, ...]],
    list[tuple[Any, ...]],
    list[tuple[Any, ...]],
]:
    profile_raw_rows: list[tuple[Any, ...]] = []
    profile_curated_rows: list[tuple[Any, ...]] = []
    media_raw_rows: list[tuple[Any, ...]] = []
    media_curated_rows: list[tuple[Any, ...]] = []

    discovered_id = discovered_payload.get("id")
    if not discovered_id:
        return profile_raw_rows, profile_curated_rows, media_raw_rows, media_curated_rows

    source_fetched_at = ingested_at
    profile_raw_rows.append(
        (
            source_ig_user_id,
            discovered_id,
            discovered_payload.get("username"),
            discovered_payload.get("name"),
            discovered_payload.get("biography"),
            discovered_payload.get("website"),
            as_nullable_uint64(discovered_payload.get("followers_count")),
            as_nullable_uint64(discovered_payload.get("follows_count")),
            as_nullable_uint64(discovered_payload.get("media_count")),
            source_fetched_at,
            json_compact(discovered_payload),
            run_id,
            ingested_at,
        )
    )
    profile_curated_rows.append(
        (
            source_ig_user_id,
            discovered_id,
            discovered_payload.get("username"),
            discovered_payload.get("name"),
            discovered_payload.get("biography"),
            discovered_payload.get("website"),
            as_nullable_uint64(discovered_payload.get("followers_count")),
            as_nullable_uint64(discovered_payload.get("follows_count")),
            as_nullable_uint64(discovered_payload.get("media_count")),
            source_fetched_at,
            ingested_at,
        )
    )

    media_items = discovered_payload.get("media", {}).get("data", [])
    if isinstance(media_items, list):
        for media in media_items:
            if not isinstance(media, dict):
                continue
            media_id = media.get("id")
            if not media_id:
                continue
            source_ts = parse_graph_timestamp(media.get("timestamp"))
            media_raw_rows.append(
                (
                    source_ig_user_id,
                    discovered_id,
                    media_id,
                    media.get("media_type"),
                    media.get("media_product_type"),
                    media.get("permalink"),
                    media.get("caption"),
                    source_ts,
                    json_compact(media),
                    run_id,
                    ingested_at,
                )
            )
            media_curated_rows.append(
                (
                    source_ig_user_id,
                    discovered_id,
                    media_id,
                    media.get("media_type"),
                    media.get("media_product_type"),
                    media.get("permalink"),
                    media.get("caption"),
                    source_ts,
                    ingested_at,
                )
            )

    return profile_raw_rows, profile_curated_rows, media_raw_rows, media_curated_rows


def build_conversation_rows(
    ig_user_id: str,
    conversation_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    for conv in conversation_items:
        conversation_id = conv.get("id")
        if not conversation_id:
            continue
        updated_time = parse_graph_timestamp(conv.get("updated_time"))
        participants = conv.get("participants", {}).get("data", [])
        raw_rows.append(
            (
                ig_user_id,
                conversation_id,
                updated_time,
                json_compact(participants if isinstance(participants, list) else []),
                json_compact(conv),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                conversation_id,
                updated_time,
                json_compact(participants if isinstance(participants, list) else []),
                ingested_at,
            )
        )
    return raw_rows, curated_rows


def _extract_to_ids(message: dict[str, Any]) -> list[str]:
    to_obj = message.get("to")
    if isinstance(to_obj, dict):
        data = to_obj.get("data")
        if isinstance(data, list):
            return [str(item.get("id")) for item in data if isinstance(item, dict) and item.get("id")]
    to_ids = message.get("to_ids")
    if isinstance(to_ids, list):
        return [str(item) for item in to_ids if item]
    return []


def _extract_message_text(message: dict[str, Any]) -> str | None:
    for key in ("text", "message"):
        value = message.get(key)
        if isinstance(value, str):
            return value
    return None


def build_message_rows(
    ig_user_id: str,
    conversation_id: str,
    message_items: list[dict[str, Any]],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], list[str]]:
    raw_rows: list[tuple[Any, ...]] = []
    curated_rows: list[tuple[Any, ...]] = []
    message_ids: list[str] = []
    for message in message_items:
        message_id = message.get("id")
        if not message_id:
            continue
        message_ids.append(message_id)
        from_id = None
        from_obj = message.get("from")
        if isinstance(from_obj, dict):
            from_id = from_obj.get("id")
        created_time = parse_graph_timestamp(message.get("created_time"))
        to_ids_json = json_compact(_extract_to_ids(message))
        text = _extract_message_text(message)
        is_echo = as_nullable_uint8(message.get("is_echo"))
        raw_rows.append(
            (
                ig_user_id,
                conversation_id,
                message_id,
                from_id,
                to_ids_json,
                text,
                created_time,
                is_echo,
                json_compact(message),
                run_id,
                ingested_at,
            )
        )
        curated_rows.append(
            (
                ig_user_id,
                conversation_id,
                message_id,
                from_id,
                to_ids_json,
                text,
                created_time,
                is_echo,
                ingested_at,
            )
        )
    return raw_rows, curated_rows, message_ids


def build_message_detail_rows(
    ig_user_id: str,
    message_payload: dict[str, Any],
    run_id: str,
    ingested_at: datetime,
) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    message_id = message_payload.get("id")
    if not message_id:
        return [], []
    conversation_id = None
    conversation_obj = message_payload.get("conversation")
    if isinstance(conversation_obj, dict):
        conversation_id = conversation_obj.get("id")
    created_time = parse_graph_timestamp(message_payload.get("created_time"))
    raw_rows = [
        (
            ig_user_id,
            message_id,
            conversation_id,
            created_time,
            json_compact(message_payload),
            run_id,
            ingested_at,
        )
    ]
    curated_rows = [
        (
            ig_user_id,
            message_id,
            conversation_id,
            created_time,
            ingested_at,
        )
    ]
    return raw_rows, curated_rows
