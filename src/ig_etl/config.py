from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from .constants import DEFAULT_GRAPH_BASE, DEFAULT_GRAPH_VERSION

DEFAULT_ENV_CANDIDATES = (
    Path(".prod.env"),
    Path("scratch/.prod.env"),
)


@dataclass(frozen=True)
class SyncConfig:
    ig_user_id: str
    graph_token: str
    graph_base: str
    graph_version: str
    backfill_days: int
    lookback_hours: int
    media_page_size: int
    max_media_insight_requests: int
    disable_comments: bool
    comments_page_size: int
    comments_media_scan_limit: int
    comments_lookback_hours: int
    comments_backfill_days: int
    initial_sync_start_at: datetime | None
    backfill_chunk_days: int
    max_windows_per_run: int
    lock_file: str
    http_timeout_seconds: int
    ch_host: str
    ch_port: int
    ch_username: str
    ch_password: str
    ch_database: str
    ch_secure: bool


def _require(name: str, value: str | None) -> str:
    if value:
        return value
    raise ValueError(f"Missing required setting: {name}")


def _strip_inline_comment(value: str) -> str:
    for idx, ch in enumerate(value):
        if ch == "#" and idx > 0 and value[idx - 1].isspace():
            return value[:idx].rstrip()
    return value


def _unquote(value: str) -> str:
    stripped = value.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in {"'", '"'}:
        return stripped[1:-1]
    return stripped


def _parse_utc_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"Invalid datetime format for INITIAL_SYNC_START_AT/--initial-sync-start-at: {value}"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_env_file(path: str, override: bool = False) -> bool:
    file_path = Path(path)
    if not file_path.exists():
        return False

    with file_path.open(encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = _strip_inline_comment(value).strip()
            if not key:
                continue
            value = _unquote(value)
            if override or key not in os.environ:
                os.environ[key] = value
    return True


def _load_default_env_files(override: bool = False) -> list[str]:
    loaded: list[str] = []
    for candidate in DEFAULT_ENV_CANDIDATES:
        if load_env_file(str(candidate), override=override):
            loaded.append(str(candidate))
    return loaded


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Instagram -> ClickHouse incremental sync"
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Optional env file path. If omitted, auto-loads .prod.env then scratch/.prod.env",
    )
    parser.add_argument(
        "--override-env",
        action="store_true",
        help="Override existing process env vars with values from env file(s)",
    )
    parser.add_argument("--ig-user-id", default=os.getenv("IG_USER_ID"))
    parser.add_argument(
        "--ig-graph-token",
        "--graph-token",
        dest="graph_token",
        default=os.getenv("IG_GRAPH_TOKEN"),
    )
    parser.add_argument(
        "--graph-base",
        default=os.getenv("GRAPH_BASE", DEFAULT_GRAPH_BASE),
    )
    parser.add_argument(
        "--graph-version",
        default=os.getenv("IG_GRAPH_VERSION", DEFAULT_GRAPH_VERSION),
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=int(os.getenv("BACKFILL_DAYS", "90")),
    )
    parser.add_argument(
        "--initial-sync-start-at",
        default=os.getenv("INITIAL_SYNC_START_AT"),
        help="UTC ISO datetime for initial bootstrap start, e.g. 2024-01-01T00:00:00Z",
    )
    parser.add_argument(
        "--backfill-chunk-days",
        type=int,
        default=int(os.getenv("BACKFILL_CHUNK_DAYS", "60")),
    )
    parser.add_argument(
        "--max-windows-per-run",
        type=int,
        default=int(os.getenv("MAX_WINDOWS_PER_RUN", "0")),
        help="Limit windows processed per run (0 = no limit)",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=int(os.getenv("SYNC_LOOKBACK_HOURS", "72")),
    )
    parser.add_argument(
        "--media-page-size",
        type=int,
        default=int(os.getenv("MEDIA_PAGE_SIZE", "100")),
    )
    parser.add_argument(
        "--max-media-insight-requests",
        type=int,
        default=int(os.getenv("MAX_MEDIA_INSIGHT_REQUESTS", "200")),
    )
    parser.add_argument("--disable-comments", action="store_true")
    parser.add_argument(
        "--comments-page-size",
        type=int,
        default=int(os.getenv("COMMENTS_PAGE_SIZE", "50")),
    )
    parser.add_argument(
        "--comments-media-scan-limit",
        type=int,
        default=int(os.getenv("COMMENTS_MEDIA_SCAN_LIMIT", "200")),
    )
    parser.add_argument(
        "--comments-lookback-hours",
        type=int,
        default=int(
            os.getenv("COMMENTS_LOOKBACK_HOURS", os.getenv("SYNC_LOOKBACK_HOURS", "72"))
        ),
    )
    parser.add_argument(
        "--comments-backfill-days",
        type=int,
        default=int(os.getenv("COMMENTS_BACKFILL_DAYS", os.getenv("BACKFILL_DAYS", "90"))),
    )
    parser.add_argument(
        "--lock-file",
        default=os.getenv("SYNC_LOCK_FILE", "/tmp/ig_etl_sync.lock"),
    )
    parser.add_argument(
        "--http-timeout-seconds",
        type=int,
        default=int(os.getenv("HTTP_TIMEOUT_SECONDS", "45")),
    )

    parser.add_argument("--ch-host", default=os.getenv("CH_HOST"))
    parser.add_argument("--ch-port", type=int, default=int(os.getenv("CH_PORT", "8123")))
    parser.add_argument("--ch-username", default=os.getenv("CH_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CH_PASSWORD", ""))
    parser.add_argument("--ch-database", default=os.getenv("CH_DATABASE", "instagram_etl"))
    parser.add_argument(
        "--ch-secure",
        action="store_true",
        default=os.getenv("CH_SECURE", "false").lower() == "true",
    )
    return parser


def parse_config(argv: Sequence[str] | None = None) -> SyncConfig:
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument("--env-file")
    pre_parser.add_argument("--override-env", action="store_true")
    pre_args, _ = pre_parser.parse_known_args(argv)

    if pre_args.env_file:
        if not load_env_file(pre_args.env_file, override=pre_args.override_env):
            raise ValueError(f"Env file not found: {pre_args.env_file}")
    else:
        _load_default_env_files(override=pre_args.override_env)

    args = build_parser().parse_args(argv)
    return SyncConfig(
        ig_user_id=_require("IG_USER_ID/--ig-user-id", args.ig_user_id),
        graph_token=_require("IG_GRAPH_TOKEN/--ig-graph-token", args.graph_token),
        graph_base=args.graph_base,
        graph_version=args.graph_version,
        backfill_days=args.backfill_days,
        lookback_hours=args.lookback_hours,
        media_page_size=args.media_page_size,
        max_media_insight_requests=args.max_media_insight_requests,
        disable_comments=args.disable_comments,
        comments_page_size=max(1, min(args.comments_page_size, 50)),
        comments_media_scan_limit=args.comments_media_scan_limit,
        comments_lookback_hours=args.comments_lookback_hours,
        comments_backfill_days=args.comments_backfill_days,
        initial_sync_start_at=_parse_utc_datetime(args.initial_sync_start_at),
        backfill_chunk_days=max(1, args.backfill_chunk_days),
        max_windows_per_run=max(0, args.max_windows_per_run),
        lock_file=args.lock_file,
        http_timeout_seconds=args.http_timeout_seconds,
        ch_host=_require("CH_HOST/--ch-host", args.ch_host),
        ch_port=args.ch_port,
        ch_username=args.ch_username,
        ch_password=args.ch_password,
        ch_database=args.ch_database,
        ch_secure=args.ch_secure,
    )
