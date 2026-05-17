from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class GraphAPIError(Exception):
    message: str
    status_code: int
    code: int | None = None
    subcode: int | None = None
    payload: dict[str, Any] | None = None

    def __str__(self) -> str:
        parts = [self.message, f"status={self.status_code}"]
        if self.code is not None:
            parts.append(f"code={self.code}")
        if self.subcode is not None:
            parts.append(f"subcode={self.subcode}")
        return " | ".join(parts)


def is_permission_error(exc: GraphAPIError) -> bool:
    return exc.code in {10, 200, 190}


def is_metric_validation_error(exc: GraphAPIError) -> bool:
    if exc.code != 100:
        return False
    return "metric" in exc.message.lower()


def is_field_validation_error(exc: GraphAPIError) -> bool:
    if exc.code != 100:
        return False
    message = exc.message.lower()
    return any(
        token in message
        for token in (
            "field",
            "nonexisting",
            "unsupported",
            "parameter",
            "since",
            "until",
            "unknown path",
            "does not exist",
        )
    )


def graph_get_json(
    client: httpx.Client,
    graph_base: str,
    graph_version: str,
    token: str,
    path_or_url: str,
    params: dict[str, Any] | None = None,
    retries: int = 5,
) -> dict[str, Any]:
    is_absolute = path_or_url.startswith("https://")
    url = (
        path_or_url
        if is_absolute
        else f"{graph_base.rstrip('/')}/{graph_version}/{path_or_url.lstrip('/')}"
    )
    query: dict[str, Any] | None = None
    if is_absolute:
        if "access_token=" not in path_or_url:
            query = {"access_token": token}
    else:
        query = dict(params or {})
        query["access_token"] = token

    attempt = 0
    while True:
        attempt += 1
        try:
            response = client.get(url, params=query)
        except httpx.RequestError as exc:
            if attempt <= retries:
                sleep_seconds = min(60, 2 ** (attempt - 1))
                print(f"[WARN] network error: {exc}. retrying in {sleep_seconds}s")
                time.sleep(sleep_seconds)
                continue
            raise GraphAPIError(str(exc), status_code=0) from exc

        if response.status_code < 400:
            try:
                return response.json()
            except ValueError as exc:
                raise GraphAPIError("invalid JSON response", response.status_code) from exc

        try:
            err_payload = response.json()
        except ValueError:
            err_payload = {"error": {"message": response.text}}

        err = err_payload.get("error", {})
        message = err.get("message", f"HTTP {response.status_code}")
        code = err.get("code")
        subcode = err.get("error_subcode")
        retryable = response.status_code in {429, 500, 502, 503, 504} or code in {
            4,
            17,
            32,
            613,
        }

        if retryable and attempt <= retries:
            sleep_seconds = min(60, 2 ** (attempt - 1))
            print(
                f"[WARN] graph error status={response.status_code} code={code}: {message}. retrying in {sleep_seconds}s"
            )
            time.sleep(sleep_seconds)
            continue

        raise GraphAPIError(
            message=message,
            status_code=response.status_code,
            code=code,
            subcode=subcode,
            payload=err_payload,
        )


def iter_graph_collection(
    client: httpx.Client,
    graph_base: str,
    graph_version: str,
    token: str,
    path: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    payload = graph_get_json(
        client,
        graph_base,
        graph_version,
        token,
        path,
        params=params,
    )

    while True:
        page_rows = payload.get("data", [])
        if isinstance(page_rows, list):
            items.extend(row for row in page_rows if isinstance(row, dict))
        paging = payload.get("paging", {})
        next_url = paging.get("next")
        if not next_url:
            break
        payload = graph_get_json(
            client,
            graph_base,
            graph_version,
            token,
            next_url,
            params=None,
        )

    return items


def try_metric_candidates(
    client: httpx.Client,
    graph_base: str,
    graph_version: str,
    token: str,
    path: str,
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    for candidate in candidates:
        try:
            payload = graph_get_json(
                client,
                graph_base,
                graph_version,
                token,
                path,
                params=candidate,
            )
            rows = payload.get("data", [])
            return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []
        except GraphAPIError as exc:
            if is_metric_validation_error(exc):
                continue
            raise
    return []

