"""HTTP client for the PyPI JSON API: polite, resilient, and testable.

* Rate limited (minimum interval between requests).
* Retries 429 and 5xx responses and network errors with exponential backoff and jitter,
  honouring Retry-After.
* Conditional requests (If-None-Match / ETag) so unchanged packages cost almost nothing.
* The transport, clock and sleep are injectable, so every behaviour is unit-testable offline.
"""
import json
import random
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass

USER_AGENT = "pypi-ecosystem-pipeline/0.1 (portfolio project; https://github.com/thehabeebops/pypi-ecosystem-pipeline)"


class FetchError(RuntimeError):
    """The request failed after all retries."""


@dataclass
class Response:
    status: int  # 200, 304 or 404
    body: dict | None
    etag: str | None


def urllib_transport(url: str, headers: dict, timeout: float):
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), b""


class RateLimiter:
    def __init__(self, rate_per_s: float, clock=time.monotonic, sleep=time.sleep):
        self.interval, self.clock, self.sleep, self.next_at = 1.0 / rate_per_s, clock, sleep, 0.0

    def wait(self) -> None:
        now = self.clock()
        if self.next_at > now:
            self.sleep(self.next_at - now)
            now = self.next_at
        self.next_at = max(now, self.next_at) + self.interval


class PyPIClient:
    URL = "https://pypi.org/pypi/{}/json"
    RETRYABLE = {0, 429, 500, 502, 503, 504}

    def __init__(self, rate_per_s=5.0, max_retries=5, transport=urllib_transport, sleep=time.sleep,
                 clock=time.monotonic, jitter=random.random, timeout=30.0):
        self.max_retries, self.transport, self.sleep, self.jitter, self.timeout = max_retries, transport, sleep, jitter, timeout
        self.limiter = RateLimiter(rate_per_s, clock, sleep)
        self.stats: Counter = Counter()

    def fetch(self, name: str, etag: str | None = None) -> Response:
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        if etag:
            headers["If-None-Match"] = etag
        for attempt in range(self.max_retries + 1):
            self.limiter.wait()
            self.stats["requests"] += 1
            try:
                status, hdrs, raw = self.transport(self.URL.format(name), headers, self.timeout)
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                status, hdrs, raw = 0, {}, b""
            hdrs = {k.lower(): v for k, v in hdrs.items()}
            if status in (200, 304, 404):
                try:
                    body = json.loads(raw) if status == 200 else None
                except ValueError as exc:
                    raise FetchError(f"{name}: invalid JSON") from exc
                return Response(status, body, hdrs.get("etag"))
            if status in self.RETRYABLE and attempt < self.max_retries:
                self.stats["retries"] += 1
                if status == 429:
                    self.stats["rate_limited"] += 1
                try:
                    delay = float(hdrs.get("retry-after", 0))
                except ValueError:
                    delay = 0.0
                delay = delay or min(30.0, 2.0 ** attempt)
                self.sleep(delay * (0.5 + self.jitter()))
                continue
            raise FetchError(f"{name}: HTTP {status}")
        raise FetchError(f"{name}: retries exhausted")
