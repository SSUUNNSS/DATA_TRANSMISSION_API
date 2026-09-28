from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlparse

import requests


@dataclass(frozen=True)
class EgnyteFile:
    name: str
    path: str
    group_id: str
    entry_id: str
    uploaded: int
    size: int
    last_modified: str


class EgnyteClient:
    """Small read-only Egnyte client for the SALA PoC."""

    def __init__(
        self,
        domain: str,
        access_token: str,
        *,
        timeout_seconds: float = 60.0,
        max_retries: int = 2,
        session: requests.Session | None = None,
    ) -> None:
        if not domain.strip():
            raise ValueError("Egnyte domain must not be empty.")
        if not access_token.strip():
            raise ValueError("Egnyte access token must not be empty.")

        self.host = _normalize_domain(domain)
        self.access_token = access_token.strip()
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = session or requests.Session()

    @property
    def base_url(self) -> str:
        return f"https://{self.host}/pubapi/v1"

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Accept": "application/json",
        }

    def list_files(
        self,
        folder_path: str,
        *,
        count: int = 20,
    ) -> list[EgnyteFile]:
        if count <= 0:
            raise ValueError("count must be greater than zero.")

        encoded_path = _encode_egnyte_path(folder_path)
        response = self._get(
            f"{self.base_url}/fs/{encoded_path}",
            params={
                "list_content": "true",
                "count": count,
                "offset": 0,
                "sort_by": "last_modified",
                "sort_direction": "descending",
            },
        )
        payload: dict[str, Any] = response.json()

        files: list[EgnyteFile] = []
        for item in payload.get("files", []):
            group_id = str(item.get("group_id", "")).strip()
            entry_id = str(item.get("entry_id", "")).strip()
            path = str(item.get("path", "")).strip()
            name = str(item.get("name", "")).strip()
            if not group_id or not entry_id or not path or not name:
                continue

            files.append(
                EgnyteFile(
                    name=name,
                    path=path,
                    group_id=group_id,
                    entry_id=entry_id,
                    uploaded=int(item.get("uploaded") or 0),
                    size=int(item.get("size") or 0),
                    last_modified=str(item.get("last_modified", "")),
                )
            )

        files.sort(key=lambda file: (file.uploaded, file.name), reverse=True)
        return files

    def download_file(self, file: EgnyteFile) -> bytes:
        """Download the exact file version returned by list_files()."""
        response = self._get(
            f"{self.base_url}/fs-content/ids/file/{quote(file.group_id, safe='')}",
            params={"entry_id": file.entry_id},
            accept_json=False,
        )
        return response.content

    def _get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        accept_json: bool = True,
    ) -> requests.Response:
        headers = dict(self.headers)
        if not accept_json:
            headers["Accept"] = "*/*"

        last_response: requests.Response | None = None
        for attempt in range(self.max_retries + 1):
            response = self.session.get(
                url,
                headers=headers,
                params=params,
                timeout=self.timeout_seconds,
            )
            last_response = response

            if response.status_code < 400:
                return response

            if response.status_code not in {429, 500, 502, 503, 504}:
                response.raise_for_status()

            if attempt >= self.max_retries:
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After", "").strip()
            try:
                delay = float(retry_after) if retry_after else 2**attempt
            except ValueError:
                delay = 2**attempt
            time.sleep(max(delay, 0.5))

        assert last_response is not None
        last_response.raise_for_status()
        return last_response


def _normalize_domain(domain: str) -> str:
    value = domain.strip().rstrip("/")
    if "://" in value:
        parsed = urlparse(value)
        if not parsed.hostname:
            raise ValueError(f"Invalid Egnyte domain: {domain!r}")
        host = parsed.hostname
    else:
        host = value.split("/", 1)[0]

    if "." not in host:
        host = f"{host}.egnyte.com"
    return host


def _encode_egnyte_path(path: str) -> str:
    stripped = path.strip().strip("/")
    if not stripped:
        raise ValueError("Egnyte folder path must not be empty.")
    return "/".join(quote(segment, safe="") for segment in stripped.split("/"))
