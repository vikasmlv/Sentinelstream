"""Replay transactions into Kafka at a configurable rate.

    python -m sentinelstream.simulator --rate 2000 --limit 100000
    python -m sentinelstream.simulator --rate 500 --dup-rate 0.02 --poison-rate 0.001   # failure demo

Reports the ACHIEVED send rate; if it is below --rate the producer (not Kafka/consumer) was the limit.
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid

import pandas as pd
from confluent_kafka import Producer

from .config import Settings


def _clean(v):
    return None if v is None or (isinstance(v, float) and v != v) else v


def build_messages(df: pd.DataFrame) -> list[tuple[bytes, bytes]]:
    out = []
    for r in df.to_dict("records"):
        ev = {"event_id": f"{uuid.uuid4()}", "user_id": str(r["user_id"]), "ts": float(r["ts"]),
              "amount": float(r["amount"]), "product_cd": r.get("product_cd") or "",
              "device_id": _clean(r.get("device_id")), "geo_id": _clean(r.get("geo_id")),
              "label": int(r["label"])}
        out.append((ev["user_id"].encode(), json.dumps(ev, separators=(",", ":")).encode()))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data/processed/events.parquet")
    ap.add_argument("--rate", type=float, default=1000.0, help="target events/sec")
    ap.add_argument("--limit", type=int, default=0, help="0 = all rows")
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--dup-rate", type=float, default=0.0, help="fraction re-sent (tests dedupe)")
    ap.add_argument("--poison-rate", type=float, default=0.0, help="fraction of garbage messages (tests DLQ)")
    ap.add_argument("--seed", type=int, default=0)
    a = ap.parse_args()
    cfg = Settings.from_env()
    rnd = random.Random(a.seed)

    df = pd.read_parquet(a.data).iloc[a.offset:]
    if a.limit:
        df = df.iloc[: a.limit]
    msgs = build_messages(df)  # pre-serialise so JSON cost doesn't distort pacing
    plan: list[tuple[bytes, bytes]] = []
    n_dup = n_poison = 0
    for k, v in msgs:
        plan.append((k, v))
        if rnd.random() < a.dup_rate:
            plan.append((k, v))
            n_dup += 1
        if rnd.random() < a.poison_rate:
            plan.append((b"poison", b"{not-json"))
            n_poison += 1

    errors = 0

    def on_delivery(err, _msg):
        nonlocal errors
        if err:
            errors += 1

    p = Producer({
        "bootstrap.servers": cfg.kafka_bootstrap, "linger.ms": 5, "compression.type": "lz4",
        "enable.idempotence": True, "acks": "all", "queue.buffering.max.messages": 500000,
    })
    n = len(plan)
    print(f"sending {n} messages ({n_dup} dup, {n_poison} poison) at target {a.rate}/s")
    start = time.perf_counter()
    sent = 0
    while sent < n:
        due = min(n, int((time.perf_counter() - start) * a.rate) + 1)
        while sent < due:
            k, v = plan[sent]
            try:
                p.produce(cfg.topic, key=k, value=v, on_delivery=on_delivery)
            except BufferError:
                p.poll(0.1)
                continue
            sent += 1
        p.poll(0)
        if sent < n:
            time.sleep(0.0005)
    p.flush()
    el = time.perf_counter() - start
    print(json.dumps({"sent": n, "elapsed_s": round(el, 2), "achieved_rate": round(n / el, 1),
                      "target_rate": a.rate, "delivery_errors": errors}))


if __name__ == "__main__":
    main()
