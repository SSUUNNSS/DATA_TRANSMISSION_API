from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging
import os
import threading
from typing import Any
from urllib.parse import quote, urlparse

import requests


class GlobalRateLimiter:
    def __init__(self, min_interval: float = 0.7) -> None:
        self.min_interval = min_interval
        self._last_request_at: float | None = None
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            if self._last_request_at is not None:
                delay = self.min_interval - (now - self._last_request_at)
                if delay > 0:
                    time.sleep(delay)
            self._last_request_at = time.monotonic()


GLOBAL_RATE_LIMITER = GlobalRateLimiter(0.7)
LOGGER = logging.getLogger("egnyte.client")
OAUTH_429_MAX_RETRIES = 3


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
        access_token: str | None = None,
        *,
        api_key: str | None = None,
        api_secret: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout_seconds: float = 60.0,
        max_retries: int = 2,
        session: requests.Session | None = None,
        rate_limiter: GlobalRateLimiter | None = None,
    ) -> None:
        if not domain.strip():
            raise ValueError("Egnyte domain must not be empty.")
        self.host = _normalize_domain(domain)
        self._access_token_override = (access_token or "").strip() or None
        self._access_token = self._access_token_override
        self._oauth_credentials = {
            "client_id": (api_key or "").strip(),
            "client_secret": (api_secret or "").strip(),
            "username": (username or "").strip(),
            "password": password or "",
        }
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.session = session or requests.Session()
        self.rate_limiter = rate_limiter or GLOBAL_RATE_LIMITER

        if self._access_token is None and not all(self._oauth_credentials.values()):
            raise ValueError(
                "Set EGNYTE_ACCESS_TOKEN or all of EGNYTE_API_KEY, EGNYTE_API_SECRET, "
                "EGNYTE_USERNAME, and EGNYTE_PASSWORD."
            )

    @classmethod
    def from_environment(cls, environ: Any = None) -> EgnyteClient:
        env = os.environ if environ is None else environ
        domain = env.get("EGNYTE_DOMAIN", "").strip()
        if not domain:
            raise ValueError("Missing EGNYTE_DOMAIN environment variable.")
        return cls(
            domain,
            env.get("EGNYTE_ACCESS_TOKEN"),
            api_key=env.get("EGNYTE_API_KEY"),
            api_secret=env.get("EGNYTE_API_SECRET"),
            username=env.get("EGNYTE_USERNAME"),
            password=env.get("EGNYTE_PASSWORD"),
        )

    @property
    def base_url(self) -> str:
        return f"https://{self.host}/pubapi/v1"

    @property
    def headers(self) -> dict[str, str]:
        self.ensure_authenticated()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Accept": "application/json",
        }

    def ensure_authenticated(self) -> None:
        """Acquire OAuth credentials once before a multi-request/site run."""
        if not self._access_token:
            self._obtain_access_token()

    def _obtain_access_token(self) -> None:
        if not all(self._oauth_credentials.values()):
            raise requests.HTTPError(
                "Egnyte returned HTTP 401, but OAuth credentials are unavailable for token refresh."
            )
        retry_count = 0
        while True:
            try:
                self.rate_limiter.wait()
                response = self.session.post(
                    f"https://{self.host}/puboauth/token",
                    data={"grant_type": "password", **self._oauth_credentials},
                    headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
                    timeout=self.timeout_seconds,
                )
            except requests.RequestException as error:
                raise requests.HTTPError("Egnyte OAuth token request failed.") from error

            if response.status_code == 429:
                if retry_count >= OAUTH_429_MAX_RETRIES:
                    raise requests.HTTPError(
                        f"Egnyte OAuth token endpoint remained rate limited after {retry_count} retries."
                    )
                delay = _oauth_retry_delay(response.headers, retry_count)
                LOGGER.warning(
                    "[EGNYTE][AUTH] token endpoint rate limited; retrying in %s seconds",
                    f"{delay:g}",
                )
                time.sleep(delay)
                retry_count += 1
                continue

            try:
                response.raise_for_status()
                token = str(response.json().get("access_token") or "").strip()
            except (requests.RequestException, ValueError, TypeError) as error:
                raise requests.HTTPError("Egnyte OAuth token request failed.") from error
            if not token:
                raise requests.HTTPError("Egnyte OAuth response did not contain an access token.")
            self._access_token = token
            return

    def list_files(
        self,
        folder_path: str,
        *,
        count: int = 1000,
    ) -> list[EgnyteFile]:
        if count <= 0:
            raise ValueError("count must be greater than zero.")

        encoded_path = _encode_egnyte_path(folder_path)
        files: list[EgnyteFile] = []
        offset = 0
        while True:
            response = self._get(
                f"{self.base_url}/fs/{encoded_path}",
                params={"list_content": "true", "count": count, "offset": offset},
            )
            payload: dict[str, Any] = response.json()
            for item in payload.get("files", []):
                entry_id = str(item.get("entry_id") or "").strip()
                name = str(item.get("name") or "").strip()
                if not entry_id or not name:
                    raise ValueError("Egnyte listing missing version ID or filename")
                files.append(EgnyteFile(
                    name=name,
                    path=str(item.get("path") or f"{folder_path.rstrip('/')}/{name}"),
                    group_id=str(item.get("group_id") or ""),
                    entry_id=entry_id,
                    uploaded=int(item.get("uploaded") or 0),
                    size=int(item.get("size") or 0),
                    last_modified=str(item.get("last_modified") or ""),
                ))
            returned = len(payload.get("files", [])) + len(payload.get("folders", []))
            offset += returned
            total = payload.get("total_count")
            if returned == 0 or (total is not None and offset >= int(total)) or (total is None and returned < count):
                break

        files.sort(key=lambda file: (file.uploaded, file.name), reverse=True)
        return files

    def download_file(self, file: EgnyteFile) -> bytes:
        """Download the exact file version returned by list_files()."""
        response = self._get(
            (f"{self.base_url}/fs-content/ids/file/{quote(file.group_id, safe='')}"
             if file.group_id else f"{self.base_url}/fs-content/{_encode_egnyte_path(file.path)}"),
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
        last_response: requests.Response | None = None
        refreshed_after_401 = False
        attempt = 0
        while True:
            headers = dict(self.headers)
            if not accept_json:
                headers["Accept"] = "*/*"
            self.rate_limiter.wait()
            try:
                response = self.session.get(
                    url, headers=headers, params=params, timeout=self.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError):
                if attempt >= self.max_retries:
                    raise
                time.sleep(2**attempt)
                attempt += 1
                continue
            last_response = response

            if response.status_code == 401:
                if refreshed_after_401:
                    raise requests.HTTPError(
                        "Egnyte API request failed with HTTP 401 after token refresh.", response=response
                    )
                self._obtain_access_token()
                refreshed_after_401 = True
                continue

            if response.status_code < 400:
                return response

            if response.status_code != 429 and not 500 <= response.status_code <= 599:
                response.raise_for_status()

            if attempt >= self.max_retries:
                response.raise_for_status()

            retry_after = response.headers.get("Retry-After", "").strip()
            try:
                delay = float(retry_after) if retry_after else 2**attempt
            except ValueError:
                delay = 2**attempt
            time.sleep(max(delay, 0.5))
            attempt += 1

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


def _oauth_retry_delay(headers: Any, retry_count: int) -> float:
    retry_after = str(headers.get("Retry-After", "")).strip()
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return float(2 ** (retry_count + 1))


def _encode_egnyte_path(path: str) -> str:
    stripped = path.strip().strip("/")
    if not stripped:
        raise ValueError("Egnyte folder path must not be empty.")
    return "/".join(quote(segment, safe="") for segment in stripped.split("/"))
