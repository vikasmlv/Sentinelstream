"""Fast-path dedupe cache in Redis.

IMPORTANT ordering: the marker is written only AFTER the decision is durable in DynamoDB.
If we marked first and crashed before the write, the redelivered message would be skipped
and the decision lost. DynamoDB `attribute_not_exists(event_id)` stays the source of truth;
this cache only saves round trips for the common duplicate case.
"""
from __future__ import annotations

import redis


class DoneCache:
    def __init__(self, client: redis.Redis, ttl_s: int = 86400):
        self.r = client
        self.ttl = ttl_s

    @staticmethod
    def _key(event_id: str) -> str:
        return f"done:{event_id}"

    def are_done(self, ids: list[str]) -> list[bool]:
        pipe = self.r.pipeline(transaction=False)
        for i in ids:
            pipe.exists(self._key(i))
        return [bool(x) for x in pipe.execute()]

    def mark_done(self, ids: list[str]) -> None:
        if not ids:
            return
        pipe = self.r.pipeline(transaction=False)
        for i in ids:
            pipe.set(self._key(i), 1, ex=self.ttl)
        pipe.execute()
