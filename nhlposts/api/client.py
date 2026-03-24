"""NHL API HTTP client with rate limiting and retry logic."""

from __future__ import annotations

import logging
import time
import threading
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)


class NHLAPIError(Exception):
    """Base class for NHL API errors."""


class RateLimitError(NHLAPIError):
    """Raised when the API returns 429."""


class NotFoundError(NHLAPIError):
    """Raised when the API returns 404."""


class ServerError(NHLAPIError):
    """Raised for 5xx responses."""


class NHLClient:
    """
    HTTP client for the NHL API.

    Enforces a minimum inter-request delay to avoid rate limiting.
    Retries transient errors with exponential backoff.
    """

    BASE = "https://api-web.nhle.com/v1"

    def __init__(self, rate_limit: float = 0.5, timeout: int = 15) -> None:
        """
        Args:
            rate_limit: Minimum seconds between requests (default 0.5 = 2 req/sec).
            timeout: HTTP request timeout in seconds.
        """
        self._rate_limit = rate_limit
        self._last_request_time: float = 0.0
        self._lock = threading.Lock()
        self._client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": "nhlposts/0.1.0 (github.com/nhlposts)"},
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "NHLClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def _wait_for_rate_limit(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._rate_limit:
                time.sleep(self._rate_limit - elapsed)
            self._last_request_time = time.monotonic()

    @retry(
        retry=retry_if_exception_type((httpx.TransportError, ServerError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Make a GET request to the NHL API.

        Args:
            path: URL path (will be joined to BASE).
            params: Optional query parameters.

        Returns:
            Parsed JSON response as a dict.

        Raises:
            NotFoundError: On 404.
            RateLimitError: On 429.
            ServerError: On 5xx.
            NHLAPIError: On other non-2xx responses.
        """
        self._wait_for_rate_limit()

        url = f"{self.BASE}{path}"
        logger.debug("GET %s params=%s", url, params)

        start = time.monotonic()
        try:
            response = self._client.get(url, params=params)
        except httpx.TransportError as exc:
            logger.warning("Transport error on %s: %s", url, exc)
            raise

        elapsed_ms = (time.monotonic() - start) * 1000
        logger.debug("  -> %d in %.0fms", response.status_code, elapsed_ms)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            raise NotFoundError(f"Not found: {url}")
        elif response.status_code == 429:
            raise RateLimitError(f"Rate limited: {url}")
        elif response.status_code >= 500:
            raise ServerError(f"Server error {response.status_code}: {url}")
        else:
            raise NHLAPIError(f"HTTP {response.status_code}: {url}")
