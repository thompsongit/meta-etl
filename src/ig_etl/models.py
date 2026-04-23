from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class SyncCounters:
    rows_extracted: int = 0
    rows_loaded_raw: int = 0
    rows_loaded_curated: int = 0


@dataclass
class MediaRows:
    raw_rows: list[tuple[Any, ...]]
    curated_rows: list[tuple[Any, ...]]
    media_ids: list[str]
    max_timestamp: datetime | None


@dataclass
class CommentRows:
    raw_rows: list[tuple[Any, ...]]
    curated_rows: list[tuple[Any, ...]]
    max_timestamp: datetime | None


@dataclass(frozen=True)
class SyncWindow:
    start: datetime
    end: datetime
