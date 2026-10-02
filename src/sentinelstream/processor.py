"""Kafka-agnostic core of the consumer: process a batch of raw messages.

Contract: when `process_batch` RETURNS, every message is in exactly one final state
(decision durable / duplicate / sampled-out / sent to DLQ), so the caller may commit offsets.
If it RAISES, the caller must NOT commit (message is redelivered => at-least-once).

Failure policy
  - invalid payload (poison)           -> DLQ immediately, no retry
  - Redis/scorer transient errors      -> retry w/ backoff, then DLQ the affected events
  - scorer 4xx (PermanentError)        -> DLQ, no retry
  - DynamoDB throttling/5xx per item   -> retry per item, then DLQ that item
"""
from __future__ import annotations

import logging
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Callable

import numpy as np
from pydantic import ValidationError

from .config import Settings
from .dedupe import DoneCache
from .feature_store import FeatureStore
from .retry import retry
from .schemas import Event

log = logging.getLogger("sentinel.processor")


@dataclass
class RawMessage:
    value: bytes
    key: bytes | None = None
    topic: str = ""
    partition: int = -1
    offset: int = -1
    timestamp_ms: int | None = None


@dataclass
class Stats:
    consumed: int = 0
    written: int = 0
    duplicates: int = 0
    sampled_out: int = 0
    dlq: int = 0
    decisions: dict = field(default_factory=lambda: {"approve": 0, "review": 0, "block": 0})
    first_ts: float | None = None
    last_ts: float | None = None


class LatencyTracker:
    def __init__(self) -> None:
        self.e2e_ms: list[float] = []  # consume -> decision durable
        self.lag_ms: list[float] = []  # Kafka append time -> decision durable (includes queueing)

    def record(self, e2e_ms: float, lag_ms: float | None) -> None:
        self.e2e_ms.append(e2e_ms)
        if lag_ms is not None:
            self.lag_ms.append(lag_ms)

    @staticmethod
    def _pct(v: list[float]) -> dict:
        if not v:
            return {"count": 0}
        a = np.asarray(v)
        return {"count": len(v), "p50": float(np.percentile(a, 50)), "p95": float(np.percentile(a, 95)),
                "p99": float(np.percentile(a, 99)), "max": float(a.max())}

    def summary(self) -> dict:
        return {"consume_to_decision_ms": self._pct(self.e2e_ms), "kafka_to_decision_ms": self._pct(self.lag_ms)}


class Processor:
    def __init__(
        self,
        cfg: Settings,
        feature_store: FeatureStore,
        done_cache: DoneCache,
        scorer,
        store,
        dlq_sink: Callable[[RawMessage, str, str], None],
    ):
        self.cfg, self.fs, self.done, self.scorer, self.store, self.dlq_sink = cfg, feature_store, done_cache, scorer, store, dlq_sink
        self.stats = Stats()
        self.latency = LatencyTracker()
        self.pool = ThreadPoolExecutor(max_workers=cfg.ddb_workers, thread_name_prefix="ddb")

    def close(self) -> None:
        self.pool.shutdown(wait=True)

    def _retry(self, fn, what: str):
        return retry(fn, attempts=self.cfg.max_retries, base_delay=self.cfg.retry_base_delay_s, what=what)

    def _to_dlq(self, m: RawMessage, reason: str, detail: str = "") -> None:
        self.stats.dlq += 1
        log.error("to DLQ", extra={"ctx": {"reason": reason, "detail": detail[:200], "partition": m.partition, "offset": m.offset}})
        self.dlq_sink(m, reason, detail)

    def _should_write(self, event_id: str, decision: str) -> bool:
        """Free-tier DynamoDB is ~25 writes/s. Optionally persist only a deterministic sample of approves."""
        if decision != "approve" or self.cfg.approve_sample_rate >= 1.0:
            return True
        return (zlib.crc32(event_id.encode()) % 10_000) / 10_000 < self.cfg.approve_sample_rate

    def _write_one(self, ev: Event, feats: dict, res: dict, kafka_ts_ms: int | None):
        decision = res["decision"]
        if not self._should_write(ev.event_id, decision):
            return "sampled_out", time.monotonic()
        item = {
            "event_id": ev.event_id, "user_id": ev.user_id, "ts": ev.ts, "amount": ev.amount,
            "decision": decision, "score": res["score"], "model_version": res["model_version"],
            "features": feats, "label": ev.label, "kafka_ts_ms": kafka_ts_ms,
            "scored_at_ms": int(time.time() * 1000),
        }
        created = self._retry(lambda: self.store.put_decision(item), "dynamodb.put")
        return ("written" if created else "duplicate"), time.monotonic()

    def process_batch(self, msgs: list[RawMessage]) -> None:
        t0 = time.monotonic()
        self.stats.consumed += len(msgs)
        if self.stats.first_ts is None:
            self.stats.first_ts = time.time()

        parsed: list[tuple[Event, RawMessage]] = []
        for m in msgs:
            try:
                parsed.append((Event.model_validate_json(m.value), m))
            except ValidationError as e:
                self._to_dlq(m, "poison:invalid_payload", str(e))
        if not parsed:
            return

        ids = [ev.event_id for ev, _ in parsed]
        try:  # cache is only an optimisation; DynamoDB conditional put remains authoritative
            done = self._retry(lambda: self.done.are_done(ids), "redis.dedupe")
        except Exception:  # noqa: BLE001
            done = [False] * len(ids)

        todo: list[tuple[Event, RawMessage]] = []
        seen: set[str] = set()
        for (ev, m), d in zip(parsed, done):
            if d or ev.event_id in seen:
                self.stats.duplicates += 1
                continue
            seen.add(ev.event_id)
            todo.append((ev, m))
        if not todo:
            return

        try:
            feats = self._retry(lambda: self.fs.update_and_get([e for e, _ in todo]), "redis.features")
        except Exception as exc:  # noqa: BLE001
            for _, m in todo:
                self._to_dlq(m, "feature_failed", repr(exc))
            return
        try:
            results = self._retry(lambda: self.scorer.score_batch([(e.event_id, f) for (e, _), f in zip(todo, feats)]), "scorer")
        except Exception as exc:  # noqa: BLE001
            for _, m in todo:
                self._to_dlq(m, "score_failed", repr(exc))
            return

        futs = [self.pool.submit(self._write_one, ev, f, r, m.timestamp_ms) for (ev, m), f, r in zip(todo, feats, results)]
        finished: list[str] = []
        now_ms = time.time() * 1000
        for fut, (ev, m), r in zip(futs, todo, results):
            try:
                status, done_at = fut.result()
            except Exception as exc:  # noqa: BLE001
                self._to_dlq(m, "write_failed", repr(exc))
                continue
            finished.append(ev.event_id)
            if status == "written":
                self.stats.written += 1
                self.stats.decisions[r["decision"]] += 1
            elif status == "duplicate":
                self.stats.duplicates += 1
            else:
                self.stats.sampled_out += 1
                self.stats.decisions[r["decision"]] += 1
            lag = (now_ms - m.timestamp_ms) if m.timestamp_ms else None
            self.latency.record((done_at - t0) * 1000, lag)
        self.stats.last_ts = time.time()
        try:
            self.done.mark_done(finished)  # AFTER durable write, never before
        except Exception:  # noqa: BLE001
            log.warning("could not mark done cache (non-fatal)")
