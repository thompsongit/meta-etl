#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ig_etl.config import load_env_file 
from ig_etl.constants import ( 
    BUSINESS_DISCOVERY_PROFILE_FIELDS_CANDIDATES,
    CHILD_MEDIA_FIELDS_CANDIDATES,
    COMMENT_FIELDS_CANDIDATES,
    COMMENT_REPLY_FIELDS_CANDIDATES,
    CONVERSATION_FIELDS_CANDIDATES,
    DEFAULT_GRAPH_BASE,
    DEFAULT_GRAPH_VERSION,
    HASHTAG_MEDIA_FIELDS_CANDIDATES,
    MEDIA_FIELDS_CANDIDATES,
    MEDIA_INSIGHT_CANDIDATES,
    MESSAGE_FIELDS_CANDIDATES,
    MENTIONED_MEDIA_FIELDS_CANDIDATES,
    STORY_FIELDS_CANDIDATES,
    TAG_FIELDS_CANDIDATES,
    USER_INSIGHT_CANDIDATES,
)
from ig_etl.graph_api import GraphAPIError, graph_get_json, try_metric_candidates  # noqa: E402
from ig_etl.utils import utc_now  # noqa: E402

try:
    import clickhouse_connect
except ModuleNotFoundError:  # pragma: no cover
    clickhouse_connect = None


PROFILE_FIELDS_CANDIDATES = [
    "id,legacy_instagram_user_id,username,name,biography,website,profile_picture_url,followers_count,follows_count,media_count",
    "id,username,name,biography,website,profile_picture_url,followers_count,follows_count,media_count",
]

@dataclass(frozen=True)
class ApiSettings:
    ig_user_id: str
    graph_token: str
    graph_base: str
    graph_version: str
    timeout_seconds: int
    media_limit: int
    media_detail_limit: int
    comments_per_media_limit: int
    hashtag_names: tuple[str, ...]
    business_discovery_usernames: tuple[str, ...]


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    username: str
    password: str
    database: str
    secure: bool
    cluster: str | None
    alt_hosts: tuple[str, ...]
    sample_rows: int
    ig_user_id: str


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    raise ValueError(f"Missing required setting: {name}")


def _parse_csv_list(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    parts = [part.strip() for part in raw.split(",")]
    return tuple(part for part in parts if part)


def _parse_optional_str(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    return value or None


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


def _clickhouse_targets(settings: ClickHouseSettings) -> list[tuple[str, int]]:
    targets: list[tuple[str, int]] = [(settings.host, settings.port)]
    for alt in settings.alt_hosts:
        targets.append(_parse_host_port(alt, settings.port))
    deduped: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for host, port in targets:
        normalized = (host.strip(), int(port))
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _build_api_settings(args: argparse.Namespace) -> ApiSettings:
    hashtag_names = (
        args.hashtag_names
        if args.hashtag_names is not None
        else os.getenv("HASHTAG_NAMES", "")
    )
    business_discovery_usernames = (
        args.business_discovery_usernames
        if args.business_discovery_usernames is not None
        else os.getenv("BUSINESS_DISCOVERY_USERNAMES", "")
    )

    return ApiSettings(
        ig_user_id=args.ig_user_id or _require_env("IG_USER_ID"),
        graph_token=args.ig_graph_token or _require_env("IG_GRAPH_TOKEN"),
        graph_base=args.graph_base or os.getenv("GRAPH_BASE", DEFAULT_GRAPH_BASE),
        graph_version=args.graph_version or os.getenv("IG_GRAPH_VERSION", DEFAULT_GRAPH_VERSION),
        timeout_seconds=args.timeout_seconds,
        media_limit=max(1, args.media_limit),
        media_detail_limit=max(1, args.media_detail_limit),
        comments_per_media_limit=max(1, args.comments_per_media_limit),
        hashtag_names=_parse_csv_list(hashtag_names),
        business_discovery_usernames=_parse_csv_list(business_discovery_usernames),
    )


def _build_ch_settings(args: argparse.Namespace) -> ClickHouseSettings:
    raw_alt_hosts = (
        args.ch_alt_hosts
        if args.ch_alt_hosts is not None
        else os.getenv("CH_ALT_HOSTS", "")
    )
    return ClickHouseSettings(
        host=args.ch_host or _require_env("CH_HOST"),
        port=int(args.ch_port or os.getenv("CH_PORT", "8123")),
        username=args.ch_username or os.getenv("CH_USER", "default"),
        password=args.ch_password if args.ch_password is not None else os.getenv("CH_PASSWORD", ""),
        database=args.ch_database or os.getenv("CH_DATABASE", "instagram_etl"),
        secure=(str(args.ch_secure).lower() == "true")
        if args.ch_secure is not None
        else os.getenv("CH_SECURE", "false").lower() == "true",
        cluster=_parse_optional_str(
            args.ch_cluster if args.ch_cluster is not None else os.getenv("CH_CLUSTER")
        ),
        alt_hosts=_parse_csv_list(raw_alt_hosts),
        sample_rows=max(1, args.raw_sample_rows),
        ig_user_id=args.ig_user_id or _require_env("IG_USER_ID"),
    )


def _verify_cluster(ch_client: Any, cluster_name: str) -> None:
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


def _connect_clickhouse(settings: ClickHouseSettings) -> Any:
    targets = _clickhouse_targets(settings)
    last_exc: Exception | None = None

    for host, port in targets:
        ch_client = None
        try:
            ch_client = clickhouse_connect.get_client(
                host=host,
                port=port,
                username=settings.username,
                password=settings.password,
                database=settings.database,
                secure=settings.secure,
            )
            ch_client.command("SELECT 1")
            if settings.cluster:
                _verify_cluster(ch_client, settings.cluster)
            return ch_client
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if ch_client is not None:
                try:
                    ch_client.close()
                except Exception:
                    pass

    target_text = ",".join(f"{host}:{port}" for host, port in targets)
    raise RuntimeError(f"Unable to connect to ClickHouse targets: {target_text}") from last_exc


def _new_schema_node() -> dict[str, Any]:
    return {"types": set(), "properties": {}, "items": None}


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"


def _add_value(node: dict[str, Any], value: Any) -> None:
    t = _json_type(value)
    node["types"].add(t)

    if t == "object" and isinstance(value, dict):
        props: dict[str, dict[str, Any]] = node["properties"]
        for key, child_value in value.items():
            child = props.get(key)
            if child is None:
                child = _new_schema_node()
                props[key] = child
            _add_value(child, child_value)

    if t == "array" and isinstance(value, list):
        if node["items"] is None:
            node["items"] = _new_schema_node()
        for item in value:
            _add_value(node["items"], item)


def _to_json_schema(node: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    node_types = sorted(node["types"])
    if not node_types:
        return out

    out["type"] = node_types[0] if len(node_types) == 1 else node_types

    if "object" in node["types"]:
        properties = {
            key: _to_json_schema(child)
            for key, child in sorted(node["properties"].items(), key=lambda kv: kv[0])
        }
        out["properties"] = properties
        out["additionalProperties"] = True

    if "array" in node["types"]:
        if node["items"] is None:
            out["items"] = {}
        else:
            out["items"] = _to_json_schema(node["items"])

    return out


def _infer_schema(samples: list[Any]) -> dict[str, Any]:
    node = _new_schema_node()
    for sample in samples:
        _add_value(node, sample)
    return _to_json_schema(node)


def _sample_object_keys(samples: list[dict[str, Any]], limit: int = 200) -> list[str]:
    keys: set[str] = set()
    for row in samples:
        if not isinstance(row, dict):
            continue
        for key in row.keys():
            keys.add(key)
            if len(keys) >= limit:
                break
        if len(keys) >= limit:
            break
    return sorted(keys)


def _endpoint_result(
    path: str,
    samples: list[dict[str, Any]],
    errors: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "path": path,
        "sample_count": len(samples),
        "sample_keys": _sample_object_keys(samples),
        "schema": _infer_schema(samples),
        "errors": errors or [],
    }


def _try_profile_payload(client: httpx.Client, settings: ApiSettings) -> dict[str, Any]:
    last_exc: GraphAPIError | None = None
    for fields in PROFILE_FIELDS_CANDIDATES:
        try:
            return graph_get_json(
                client,
                settings.graph_base,
                settings.graph_version,
                settings.graph_token,
                f"/{settings.ig_user_id}",
                params={"fields": fields},
            )
        except GraphAPIError as exc:
            last_exc = exc
            message = exc.message.lower()
            if exc.code == 100 and (
                "legacy_instagram_user_id" in message
                or "nonexisting field" in message
                or "ig_id" in message
            ):
                continue
            raise
    if last_exc:
        raise last_exc
    return {}


def _try_media_payload(client: httpx.Client, settings: ApiSettings) -> dict[str, Any]:
    last_exc: GraphAPIError | None = None
    for fields in MEDIA_FIELDS_CANDIDATES:
        try:
            return graph_get_json(
                client,
                settings.graph_base,
                settings.graph_version,
                settings.graph_token,
                f"/{settings.ig_user_id}/media",
                params={"fields": fields, "limit": settings.media_limit},
            )
        except GraphAPIError as exc:
            last_exc = exc
            message = exc.message.lower()
            if exc.code == 100 and ("field" in message or "nonexisting field" in message):
                continue
            raise
    if last_exc:
        raise last_exc
    return {}


def _try_comments_payload(
    client: httpx.Client,
    settings: ApiSettings,
    media_id: str,
) -> list[dict[str, Any]]:
    last_exc: GraphAPIError | None = None
    for fields in COMMENT_FIELDS_CANDIDATES:
        try:
            payload = graph_get_json(
                client,
                settings.graph_base,
                settings.graph_version,
                settings.graph_token,
                f"/{media_id}/comments",
                params={
                    "fields": fields,
                    "limit": settings.comments_per_media_limit,
                },
            )
            rows = payload.get("data", [])
            return [row for row in rows if isinstance(row, dict)]
        except GraphAPIError as exc:
            last_exc = exc
            if exc.code == 100:
                continue
            raise
    if last_exc:
        raise last_exc
    return []


def _is_candidate_error(exc: GraphAPIError) -> bool:
    if exc.code != 100:
        return False
    message = exc.message.lower()
    return any(
        token in message
        for token in (
            "field",
            "nonexisting field",
            "unsupported",
            "parameter",
            "since",
            "until",
            "unknown path",
            "cannot query",
            "does not exist",
        )
    )


def _try_collection_payload(
    client: httpx.Client,
    settings: ApiSettings,
    path: str,
    field_candidates: list[str],
    limit: int,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    last_exc: GraphAPIError | None = None
    base_params = dict(extra_params or {})
    for fields in field_candidates:
        params = dict(base_params)
        params["fields"] = fields
        params["limit"] = limit
        try:
            payload = graph_get_json(
                client,
                settings.graph_base,
                settings.graph_version,
                settings.graph_token,
                path,
                params=params,
            )
            rows = payload.get("data", [])
            if not isinstance(rows, list):
                return []
            return [row for row in rows if isinstance(row, dict)]
        except GraphAPIError as exc:
            last_exc = exc
            if _is_candidate_error(exc):
                continue
            raise
    if last_exc:
        raise last_exc
    return []


def run_api_audit(settings: ApiSettings) -> dict[str, Any]:
    output: dict[str, Any] = {
        "mode": "api",
        "graph_base": settings.graph_base,
        "graph_version": settings.graph_version,
        "ig_user_id": settings.ig_user_id,
        "generated_at": utc_now().isoformat(),
        "endpoints": {},
    }

    endpoint_errors: dict[str, list[str]] = {}

    with httpx.Client(timeout=settings.timeout_seconds) as client:
        profile_samples: list[dict[str, Any]] = []
        media_samples: list[dict[str, Any]] = []
        user_insight_samples: list[dict[str, Any]] = []
        media_insight_samples: list[dict[str, Any]] = []
        comment_samples: list[dict[str, Any]] = []
        child_media_samples: list[dict[str, Any]] = []
        story_samples: list[dict[str, Any]] = []
        comment_reply_samples: list[dict[str, Any]] = []
        tag_samples: list[dict[str, Any]] = []
        mentioned_media_samples: list[dict[str, Any]] = []
        hashtag_lookup_samples: list[dict[str, Any]] = []
        hashtag_top_media_samples: list[dict[str, Any]] = []
        hashtag_recent_media_samples: list[dict[str, Any]] = []
        business_discovery_profile_samples: list[dict[str, Any]] = []
        business_discovery_media_samples: list[dict[str, Any]] = []
        conversation_samples: list[dict[str, Any]] = []
        message_samples: list[dict[str, Any]] = []
        message_detail_samples: list[dict[str, Any]] = []

        try:
            profile = _try_profile_payload(client, settings)
            if isinstance(profile, dict):
                profile_samples.append(profile)
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.setdefault("ig_user_profile", []).append(str(exc))

        media_ids: list[str] = []
        try:
            media_payload = _try_media_payload(client, settings)
            rows = media_payload.get("data", []) if isinstance(media_payload, dict) else []
            media_samples = [row for row in rows if isinstance(row, dict)]
            media_ids = [str(row.get("id")) for row in media_samples if row.get("id")]
            if settings.media_detail_limit > 0:
                media_ids = media_ids[: settings.media_detail_limit]
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.setdefault("ig_media", []).append(str(exc))

        try:
            story_samples = _try_collection_payload(
                client,
                settings,
                path=f"/{settings.ig_user_id}/stories",
                field_candidates=STORY_FIELDS_CANDIDATES,
                limit=settings.media_limit,
            )
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.setdefault("ig_stories", []).append(str(exc))

        try:
            tag_samples = _try_collection_payload(
                client,
                settings,
                path=f"/{settings.ig_user_id}/tags",
                field_candidates=TAG_FIELDS_CANDIDATES,
                limit=settings.media_limit,
            )
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.setdefault("ig_user_tags", []).append(str(exc))

        try:
            mentioned_media_samples = _try_collection_payload(
                client,
                settings,
                path=f"/{settings.ig_user_id}/mentioned_media",
                field_candidates=MENTIONED_MEDIA_FIELDS_CANDIDATES,
                limit=settings.media_limit,
            )
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.setdefault("ig_mentioned_media", []).append(str(exc))

        try:
            user_insight_samples = try_metric_candidates(
                client,
                settings.graph_base,
                settings.graph_version,
                settings.graph_token,
                f"/{settings.ig_user_id}/insights",
                USER_INSIGHT_CANDIDATES,
            )
            user_insight_samples = [
                row for row in user_insight_samples if isinstance(row, dict)
            ]
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.setdefault("ig_user_insights", []).append(str(exc))

        for media_id in media_ids:
            try:
                metric_rows = try_metric_candidates(
                    client,
                    settings.graph_base,
                    settings.graph_version,
                    settings.graph_token,
                    f"/{media_id}/insights",
                    MEDIA_INSIGHT_CANDIDATES,
                )
                for row in metric_rows:
                    if isinstance(row, dict):
                        media_insight_samples.append(row)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_media_insights", []).append(
                    f"media_id={media_id} {exc}"
                )

            try:
                comments = _try_comments_payload(client, settings, media_id)
                comment_samples.extend(comments)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_comments", []).append(
                    f"media_id={media_id} {exc}"
                )

            try:
                children = _try_collection_payload(
                    client,
                    settings,
                    path=f"/{media_id}/children",
                    field_candidates=CHILD_MEDIA_FIELDS_CANDIDATES,
                    limit=settings.media_limit,
                )
                child_media_samples.extend(children)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_media_children", []).append(
                    f"media_id={media_id} {exc}"
                )

        comment_ids = [str(row.get("id")) for row in comment_samples if row.get("id")]
        for comment_id in comment_ids[: settings.media_detail_limit]:
            try:
                replies = _try_collection_payload(
                    client,
                    settings,
                    path=f"/{comment_id}/replies",
                    field_candidates=COMMENT_REPLY_FIELDS_CANDIDATES,
                    limit=settings.comments_per_media_limit,
                )
                comment_reply_samples.extend(replies)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_comment_replies", []).append(
                    f"comment_id={comment_id} {exc}"
                )

        hashtag_ids: list[str] = []
        for hashtag_name in settings.hashtag_names:
            try:
                payload = graph_get_json(
                    client,
                    settings.graph_base,
                    settings.graph_version,
                    settings.graph_token,
                    "/ig_hashtag_search",
                    params={"user_id": settings.ig_user_id, "q": hashtag_name},
                )
                rows = payload.get("data", [])
                if isinstance(rows, list):
                    samples = [row for row in rows if isinstance(row, dict)]
                    hashtag_lookup_samples.extend(samples)
                    hashtag_ids.extend(str(row.get("id")) for row in samples if row.get("id"))
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_hashtag_lookup", []).append(
                    f"hashtag={hashtag_name} {exc}"
                )

        for hashtag_id in dict.fromkeys(hashtag_ids):
            try:
                top_rows = _try_collection_payload(
                    client,
                    settings,
                    path=f"/{hashtag_id}/top_media",
                    field_candidates=HASHTAG_MEDIA_FIELDS_CANDIDATES,
                    limit=settings.media_limit,
                    extra_params={"user_id": settings.ig_user_id},
                )
                hashtag_top_media_samples.extend(top_rows)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_hashtag_top_media", []).append(
                    f"hashtag_id={hashtag_id} {exc}"
                )

            try:
                recent_rows = _try_collection_payload(
                    client,
                    settings,
                    path=f"/{hashtag_id}/recent_media",
                    field_candidates=HASHTAG_MEDIA_FIELDS_CANDIDATES,
                    limit=settings.media_limit,
                    extra_params={"user_id": settings.ig_user_id},
                )
                hashtag_recent_media_samples.extend(recent_rows)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_hashtag_recent_media", []).append(
                    f"hashtag_id={hashtag_id} {exc}"
                )

        for username in settings.business_discovery_usernames:
            found = False
            for fields in BUSINESS_DISCOVERY_PROFILE_FIELDS_CANDIDATES:
                query_field = f"business_discovery.username({username}){{{fields}}}"
                try:
                    payload = graph_get_json(
                        client,
                        settings.graph_base,
                        settings.graph_version,
                        settings.graph_token,
                        f"/{settings.ig_user_id}",
                        params={"fields": query_field},
                    )
                except GraphAPIError as exc:
                    if _is_candidate_error(exc):
                        continue
                    endpoint_errors.setdefault("ig_business_discovery_profile", []).append(
                        f"username={username} {exc}"
                    )
                    break

                discovered = payload.get("business_discovery")
                if isinstance(discovered, dict):
                    business_discovery_profile_samples.append(discovered)
                    media_rows = discovered.get("media", {}).get("data", [])
                    if isinstance(media_rows, list):
                        business_discovery_media_samples.extend(
                            [row for row in media_rows if isinstance(row, dict)]
                        )
                found = True
                break

            if not found:
                endpoint_errors.setdefault("ig_business_discovery_profile", []).append(
                    f"username={username} no payload returned"
                )

        try:
            conversation_samples = _try_collection_payload(
                client,
                settings,
                path=f"/{settings.ig_user_id}/conversations",
                field_candidates=CONVERSATION_FIELDS_CANDIDATES,
                limit=settings.media_limit,
            )
        except Exception as exc:  # noqa: BLE001
            endpoint_errors.setdefault("ig_conversations", []).append(str(exc))

        conversation_ids = [str(row.get("id")) for row in conversation_samples if row.get("id")]
        for conversation_id in conversation_ids[: settings.media_detail_limit]:
            try:
                rows = _try_collection_payload(
                    client,
                    settings,
                    path=f"/{conversation_id}/messages",
                    field_candidates=MESSAGE_FIELDS_CANDIDATES,
                    limit=settings.media_limit,
                )
                message_samples.extend(rows)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_messages", []).append(
                    f"conversation_id={conversation_id} {exc}"
                )

        message_ids = [str(row.get("id")) for row in message_samples if row.get("id")]
        for message_id in message_ids[: settings.media_detail_limit]:
            try:
                payload = graph_get_json(
                    client,
                    settings.graph_base,
                    settings.graph_version,
                    settings.graph_token,
                    f"/{message_id}",
                    params={"fields": "id,created_time,conversation"},
                )
                if isinstance(payload, dict):
                    message_detail_samples.append(payload)
            except Exception as exc:  # noqa: BLE001
                endpoint_errors.setdefault("ig_message_detail", []).append(
                    f"message_id={message_id} {exc}"
                )

        output["endpoints"]["ig_user_profile"] = _endpoint_result(
            path="GET /{ig_user_id}",
            samples=profile_samples,
            errors=endpoint_errors.get("ig_user_profile", []),
        )
        output["endpoints"]["ig_media"] = _endpoint_result(
            path="GET /{ig_user_id}/media",
            samples=media_samples,
            errors=endpoint_errors.get("ig_media", []),
        )
        output["endpoints"]["ig_user_insights"] = _endpoint_result(
            path="GET /{ig_user_id}/insights",
            samples=user_insight_samples,
            errors=endpoint_errors.get("ig_user_insights", []),
        )
        output["endpoints"]["ig_media_insights"] = _endpoint_result(
            path="GET /{ig_media_id}/insights",
            samples=media_insight_samples,
            errors=endpoint_errors.get("ig_media_insights", []),
        )
        output["endpoints"]["ig_comments"] = _endpoint_result(
            path="GET /{ig_media_id}/comments",
            samples=comment_samples,
            errors=endpoint_errors.get("ig_comments", []),
        )
        output["endpoints"]["ig_media_children"] = _endpoint_result(
            path="GET /{ig_media_id}/children",
            samples=child_media_samples,
            errors=endpoint_errors.get("ig_media_children", []),
        )
        output["endpoints"]["ig_stories"] = _endpoint_result(
            path="GET /{ig_user_id}/stories",
            samples=story_samples,
            errors=endpoint_errors.get("ig_stories", []),
        )
        output["endpoints"]["ig_comment_replies"] = _endpoint_result(
            path="GET /{ig_comment_id}/replies",
            samples=comment_reply_samples,
            errors=endpoint_errors.get("ig_comment_replies", []),
        )
        output["endpoints"]["ig_user_tags"] = _endpoint_result(
            path="GET /{ig_user_id}/tags",
            samples=tag_samples,
            errors=endpoint_errors.get("ig_user_tags", []),
        )
        output["endpoints"]["ig_mentioned_media"] = _endpoint_result(
            path="GET /{ig_user_id}/mentioned_media",
            samples=mentioned_media_samples,
            errors=endpoint_errors.get("ig_mentioned_media", []),
        )
        output["endpoints"]["ig_hashtag_lookup"] = _endpoint_result(
            path="GET /ig_hashtag_search",
            samples=hashtag_lookup_samples,
            errors=endpoint_errors.get("ig_hashtag_lookup", []),
        )
        output["endpoints"]["ig_hashtag_top_media"] = _endpoint_result(
            path="GET /{ig_hashtag_id}/top_media",
            samples=hashtag_top_media_samples,
            errors=endpoint_errors.get("ig_hashtag_top_media", []),
        )
        output["endpoints"]["ig_hashtag_recent_media"] = _endpoint_result(
            path="GET /{ig_hashtag_id}/recent_media",
            samples=hashtag_recent_media_samples,
            errors=endpoint_errors.get("ig_hashtag_recent_media", []),
        )
        output["endpoints"]["ig_business_discovery_profile"] = _endpoint_result(
            path="GET /{ig_user_id}?fields=business_discovery.username(...)",
            samples=business_discovery_profile_samples,
            errors=endpoint_errors.get("ig_business_discovery_profile", []),
        )
        output["endpoints"]["ig_business_discovery_media"] = _endpoint_result(
            path="GET /{ig_user_id}?fields=business_discovery.username(...){media{...}}",
            samples=business_discovery_media_samples,
            errors=endpoint_errors.get(
                "ig_business_discovery_media",
                endpoint_errors.get("ig_business_discovery_profile", []),
            ),
        )
        output["endpoints"]["ig_conversations"] = _endpoint_result(
            path="GET /{ig_user_id}/conversations",
            samples=conversation_samples,
            errors=endpoint_errors.get("ig_conversations", []),
        )
        output["endpoints"]["ig_messages"] = _endpoint_result(
            path="GET /{conversation_id}/messages",
            samples=message_samples,
            errors=endpoint_errors.get("ig_messages", []),
        )
        output["endpoints"]["ig_message_detail"] = _endpoint_result(
            path="GET /{message_id}",
            samples=message_detail_samples,
            errors=endpoint_errors.get("ig_message_detail", []),
        )
        output["endpoints"]["ig_webhook_events"] = _endpoint_result(
            path="WEBHOOK push events (no pull endpoint)",
            samples=[],
            errors=["push-only stream; sample from raw_ig_webhook_events instead"],
        )

    return output


def _query_raw_payloads(
    ch_client: Any,
    table: str,
    ig_user_id: str,
    limit: int,
    account_column: str = "ig_user_id",
    time_column: str = "ingested_at",
) -> list[dict[str, Any]]:
    query = f"""
        SELECT payload_json
        FROM {table}
        WHERE {account_column} = {{ig_user_id:String}}
        ORDER BY {time_column} DESC
        LIMIT {{sample_limit:UInt32}}
    """
    rows = ch_client.query(
        query,
        parameters={"ig_user_id": ig_user_id, "sample_limit": limit},
    ).result_rows
    payloads: list[dict[str, Any]] = []
    for row in rows:
        if not row:
            continue
        try:
            payload = json.loads(row[0])
            if isinstance(payload, dict):
                payloads.append(payload)
        except Exception:
            continue
    return payloads


def run_raw_audit(settings: ClickHouseSettings) -> dict[str, Any]:
    if clickhouse_connect is None:
        raise RuntimeError("clickhouse_connect is not installed. pip install -r requirements.txt")

    output: dict[str, Any] = {
        "mode": "raw_clickhouse",
        "database": settings.database,
        "cluster": settings.cluster,
        "ig_user_id": settings.ig_user_id,
        "generated_at": utc_now().isoformat(),
        "tables": {},
    }

    table_specs: dict[str, dict[str, str]] = {
        "raw_ig_user_profile": {
            "source_endpoint": "GET /{ig_user_id}",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_media": {
            "source_endpoint": "GET /{ig_user_id}/media",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_media_children": {
            "source_endpoint": "GET /{ig_media_id}/children",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_stories": {
            "source_endpoint": "GET /{ig_user_id}/stories",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_user_insights": {
            "source_endpoint": "GET /{ig_user_id}/insights",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_media_insights": {
            "source_endpoint": "GET /{ig_media_id}/insights",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_comments": {
            "source_endpoint": "GET /{ig_media_id}/comments",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_comment_replies": {
            "source_endpoint": "GET /{ig_comment_id}/replies",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_user_tags": {
            "source_endpoint": "GET /{ig_user_id}/tags",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_mentioned_media": {
            "source_endpoint": "GET /{ig_user_id}/mentioned_media",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_hashtag_lookup": {
            "source_endpoint": "GET /ig_hashtag_search",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_hashtag_top_media": {
            "source_endpoint": "GET /{ig_hashtag_id}/top_media",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_hashtag_recent_media": {
            "source_endpoint": "GET /{ig_hashtag_id}/recent_media",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_business_discovery_profile": {
            "source_endpoint": "GET /{ig_user_id}?fields=business_discovery.username(...)",
            "account_column": "source_ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_business_discovery_media": {
            "source_endpoint": "GET /{ig_user_id}?fields=business_discovery.username(...){media{...}}",
            "account_column": "source_ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_conversations": {
            "source_endpoint": "GET /{ig_user_id}/conversations",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_messages": {
            "source_endpoint": "GET /{conversation_id}/messages",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_message_detail": {
            "source_endpoint": "GET /{message_id}",
            "account_column": "ig_user_id",
            "time_column": "ingested_at",
        },
        "raw_ig_webhook_events": {
            "source_endpoint": "WEBHOOK push events",
            "account_column": "ig_user_id",
            "time_column": "received_at",
        },
    }

    ch_client = _connect_clickhouse(settings)
    try:
        for table, spec in table_specs.items():
            errors: list[str] = []
            samples: list[dict[str, Any]] = []
            try:
                samples = _query_raw_payloads(
                    ch_client,
                    table,
                    settings.ig_user_id,
                    settings.sample_rows,
                    account_column=spec["account_column"],
                    time_column=spec["time_column"],
                )
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))

            output["tables"][table] = {
                "source_endpoint": spec["source_endpoint"],
                "sample_count": len(samples),
                "sample_keys": _sample_object_keys(samples),
                "schema": _infer_schema(samples),
                "errors": errors,
            }
    finally:
        ch_client.close()

    return output


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dump inferred JSON schemas per Instagram endpoint from API samples "
            "and/or payload_json stored in ClickHouse raw tables."
        )
    )
    parser.add_argument("--env-file", default=".prod.env")
    parser.add_argument("--output-dir", default="scratch/source_schema_dump")
    parser.add_argument("--mode", choices=["api", "raw", "both"], default="both")

    parser.add_argument("--ig-user-id", default=None)
    parser.add_argument("--ig-graph-token", default=None)
    parser.add_argument("--graph-base", default=None)
    parser.add_argument("--graph-version", default=None)
    parser.add_argument("--timeout-seconds", type=int, default=45)

    parser.add_argument("--media-limit", type=int, default=25)
    parser.add_argument("--media-detail-limit", type=int, default=10)
    parser.add_argument("--comments-per-media-limit", type=int, default=25)
    parser.add_argument("--hashtag-names", default=None)
    parser.add_argument("--business-discovery-usernames", default=None)

    parser.add_argument("--ch-host", default=None)
    parser.add_argument("--ch-port", default=None)
    parser.add_argument("--ch-username", default=None)
    parser.add_argument("--ch-password", default=None)
    parser.add_argument("--ch-database", default=None)
    parser.add_argument("--ch-secure", default=None)
    parser.add_argument("--ch-cluster", default=None)
    parser.add_argument("--ch-alt-hosts", default=None)
    parser.add_argument("--raw-sample-rows", type=int, default=1000)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    env_candidates: list[Path] = []
    if args.env_file:
        env_candidates.append(Path(args.env_file))
    for candidate in (Path(".prod.env"), Path("scratch/.prod.env")):
        if candidate not in env_candidates:
            env_candidates.append(candidate)

    for env_file in env_candidates:
        if env_file.exists():
            load_env_file(str(env_file), override=False)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    final_output: dict[str, Any] = {
        "generated_at": utc_now().astimezone(timezone.utc).isoformat(),
        "mode": args.mode,
    }

    if args.mode in {"api", "both"}:
        api_settings = _build_api_settings(args)
        final_output["api"] = run_api_audit(api_settings)

    if args.mode in {"raw", "both"}:
        ch_settings = _build_ch_settings(args)
        final_output["raw"] = run_raw_audit(ch_settings)

    all_path = output_dir / "schema_audit.json"
    with all_path.open("w", encoding="utf-8") as fh:
        json.dump(final_output, fh, indent=2, ensure_ascii=True)

    if "api" in final_output:
        api_path = output_dir / "schema_audit_api.json"
        with api_path.open("w", encoding="utf-8") as fh:
            json.dump(final_output["api"], fh, indent=2, ensure_ascii=True)
        for endpoint_name, payload in final_output["api"].get("endpoints", {}).items():
            endpoint_path = output_dir / f"schema_api__{endpoint_name}.json"
            with endpoint_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=True)

    if "raw" in final_output:
        raw_path = output_dir / "schema_audit_raw.json"
        with raw_path.open("w", encoding="utf-8") as fh:
            json.dump(final_output["raw"], fh, indent=2, ensure_ascii=True)
        for table_name, payload in final_output["raw"].get("tables", {}).items():
            table_path = output_dir / f"schema_raw__{table_name}.json"
            with table_path.open("w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2, ensure_ascii=True)

    print(f"[INFO] wrote {all_path}")
    if "api" in final_output:
        print(f"[INFO] wrote {output_dir / 'schema_audit_api.json'}")
    if "raw" in final_output:
        print(f"[INFO] wrote {output_dir / 'schema_audit_raw.json'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
