from __future__ import annotations

from typing import Sequence

from .config import parse_config
from .pipeline import run_sync

try:
    import clickhouse_connect
except ModuleNotFoundError:  # pragma: no cover - runtime dependency guard
    clickhouse_connect = None

try:
    import httpx
except ModuleNotFoundError:  # pragma: no cover - runtime dependency guard
    httpx = None


def main(argv: Sequence[str] | None = None) -> int:
    if clickhouse_connect is None or httpx is None:
        print("Missing runtime dependencies. Run: pip install -r requirements.txt")
        return 2
    try:
        config = parse_config(argv)
    except ValueError as exc:
        print(str(exc))
        return 2
    return run_sync(
        config=config,
        clickhouse_connect_module=clickhouse_connect,
        httpx_module=httpx,
    )

