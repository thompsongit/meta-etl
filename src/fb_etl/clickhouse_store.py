from __future__ import annotations

from typing import Any

from ig_etl.clickhouse_store import (
    claim_stream_window,
    complete_stream_window,
    insert_rows,
    insert_run_step,
    insert_sync_run,
    load_state,
)


def get_recent_post_ids(
    ch_client: Any,
    fb_page_id: str,
    limit: int,
) -> list[str]:
    if limit <= 0:
        return []
    query = """
        SELECT fb_post_id
        FROM curated_fb_page_posts_current
        WHERE fb_page_id = {fb_page_id:String}
        ORDER BY coalesce(source_updated_at, source_created_at, version_ts) DESC
        LIMIT {post_limit:UInt32}
    """
    result = ch_client.query(
        query,
        parameters={
            "fb_page_id": fb_page_id,
            "post_limit": limit,
        },
    ).result_rows
    return [row[0] for row in result if row and row[0]]


__all__ = [
    "claim_stream_window",
    "complete_stream_window",
    "insert_rows",
    "insert_run_step",
    "insert_sync_run",
    "load_state",
    "get_recent_post_ids",
]

