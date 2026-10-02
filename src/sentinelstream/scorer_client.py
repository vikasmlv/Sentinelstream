from __future__ import annotations

import httpx

from .retry import PermanentError


class ScorerClient:
    """Keep-alive HTTP client to the scoring service (one connection pool per consumer)."""

    def __init__(self, base_url: str, timeout_s: float = 2.0):
        self.http = httpx.Client(
            base_url=base_url, timeout=timeout_s,
            limits=httpx.Limits(max_connections=8, max_keepalive_connections=8),
        )

    def score_batch(self, items: list[tuple[str, dict]]) -> list[dict]:
        r = self.http.post("/score_batch", json={"items": [{"event_id": i, "features": f} for i, f in items]})
        if 400 <= r.status_code < 500:
            raise PermanentError(f"scorer rejected batch: {r.status_code} {r.text[:200]}")
        r.raise_for_status()  # 5xx => retried
        res = r.json()["results"]
        if len(res) != len(items):
            raise RuntimeError("scorer returned wrong result count")
        return res

    def close(self) -> None:
        self.http.close()
