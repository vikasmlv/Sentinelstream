"""Redis sliding-window velocity features.

Per key one ZSET: member = event_id, score = event ts.
  ZADD (idempotent for same member)  -> replay-safe
  ZREMRANGEBYSCORE -inf (ts-MAX)     -> bounded memory
  EXPIRE                              -> idle keys vanish
  ZCOUNT (ts-W ts]                    -> window counts
Everything for a whole poll-batch goes in ONE pipeline (1 network round trip).
Commands in a pipeline execute in order, so per-user ordering semantics hold.
"""
from __future__ import annotations

import redis

from .features import MAX_WINDOW_S, WINDOWS_S, static_features
from .schemas import Event


class FeatureStore:
    def __init__(self, client: redis.Redis, key_ttl_s: int = 2 * MAX_WINDOW_S):
        self.r = client
        self.ttl = key_ttl_s

    @staticmethod
    def user_key(v: str) -> str:
        return f"vel:u:{v}"

    @staticmethod
    def device_key(v: str) -> str:
        return f"vel:d:{v}"

    @staticmethod
    def geo_key(v: str) -> str:
        return f"vel:g:{v}"

    def _touch(self, pipe, key: str, member: str, ts: float) -> None:
        pipe.zadd(key, {member: ts})
        pipe.zremrangebyscore(key, "-inf", ts - MAX_WINDOW_S)
        pipe.expire(key, self.ttl)

    @staticmethod
    def _zcount(pipe, key: str, ts: float, window: int) -> None:
        pipe.zcount(key, f"({ts - window!r}", ts)

    def update_and_get(self, events: list[Event]) -> list[dict[str, float]]:
        pipe = self.r.pipeline(transaction=False)
        plan: list[tuple[bool, bool]] = []
        for ev in events:
            uk = self.user_key(ev.user_id)
            self._touch(pipe, uk, ev.event_id, ev.ts)
            for w in WINDOWS_S:
                self._zcount(pipe, uk, ev.ts, w)
            has_dev, has_geo = bool(ev.device_id), bool(ev.geo_id)
            if has_dev:
                dk = self.device_key(ev.device_id)
                self._touch(pipe, dk, ev.event_id, ev.ts)
                self._zcount(pipe, dk, ev.ts, 3600)
            if has_geo:
                gk = self.geo_key(ev.geo_id)
                self._touch(pipe, gk, ev.event_id, ev.ts)
                self._zcount(pipe, gk, ev.ts, 3600)
            plan.append((has_dev, has_geo))
        res = pipe.execute()

        out: list[dict[str, float]] = []
        p = 0
        for ev, (has_dev, has_geo) in zip(events, plan):
            p += 3  # zadd, zrem, expire
            c1, c10, c60 = res[p], res[p + 1], res[p + 2]
            p += 3
            dev = geo = 0
            if has_dev:
                p += 3
                dev = res[p]
                p += 1
            if has_geo:
                p += 3
                geo = res[p]
                p += 1
            f = static_features(ev.amount, ev.product_cd, ev.ts)
            f.update(
                cnt_1m=float(c1),
                cnt_10m=float(c10),
                cnt_60m=float(c60),
                dev_cnt_60m=float(dev),
                geo_cnt_60m=float(geo),
            )
            out.append(f)
        return out
