"""Re-inject DLQ messages into the main topic after the root cause is fixed.
    python -m sentinelstream.replay_dlq --max 1000
Poison payloads (invalid JSON) will just land in the DLQ again; fix or discard those manually."""
from __future__ import annotations

import argparse

from confluent_kafka import Consumer, Producer

from .config import Settings


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=1000)
    a = ap.parse_args()
    cfg = Settings.from_env()
    c = Consumer({"bootstrap.servers": cfg.kafka_bootstrap, "group.id": "dlq-replayer",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([cfg.dlq_topic])
    p = Producer({"bootstrap.servers": cfg.kafka_bootstrap})
    n = 0
    while n < a.max:
        m = c.poll(3.0)
        if m is None:
            break
        if m.error():
            continue
        p.produce(cfg.topic, key=m.key(), value=m.value())
        n += 1
    p.flush()
    c.commit(asynchronous=False)
    c.close()
    print(f"replayed {n} messages from {cfg.dlq_topic} to {cfg.topic}")


if __name__ == "__main__":
    main()
