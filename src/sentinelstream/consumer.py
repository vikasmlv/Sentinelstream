"""Kafka consumer entrypoint: python -m sentinelstream.consumer

manual offset commit AFTER Processor.process_batch returns => at-least-once delivery.
Idempotent writes (Redis ZADD by event_id, DynamoDB conditional put) => effectively-once results.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import time

import redis
from confluent_kafka import Consumer, KafkaError, Producer

from .config import Settings
from .decision_store import DecisionStore
from .dedupe import DoneCache
from .feature_store import FeatureStore
from .logging_conf import setup_logging
from .processor import Processor, RawMessage
from .scorer_client import ScorerClient

log = logging.getLogger("sentinel.consumer")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--idle-exit-seconds", type=float, default=0.0,
                    help="exit after this many idle seconds once data was seen (benchmark mode); 0 = run forever")
    args = ap.parse_args()
    cfg = Settings.from_env()
    setup_logging(cfg.log_level)

    r = redis.Redis.from_url(cfg.redis_url)
    dlq = Producer({"bootstrap.servers": cfg.kafka_bootstrap, "enable.idempotence": True})

    def dlq_sink(m: RawMessage, reason: str, detail: str) -> None:
        dlq.produce(cfg.dlq_topic, key=m.key, value=m.value, headers=[
            ("error_reason", reason.encode()), ("error_detail", detail[:500].encode()),
            ("src_topic", m.topic.encode()), ("src_partition", str(m.partition).encode()),
            ("src_offset", str(m.offset).encode()),
        ])

    proc = Processor(cfg, FeatureStore(r), DoneCache(r, cfg.dedupe_ttl_s), ScorerClient(cfg.scorer_url),
                     DecisionStore(cfg.decisions_table, cfg.aws_region, cfg.dynamodb_endpoint), dlq_sink)

    consumer = Consumer({
        "bootstrap.servers": cfg.kafka_bootstrap, "group.id": cfg.consumer_group,
        "enable.auto.commit": False, "auto.offset.reset": "latest",
        "max.poll.interval.ms": 300000, "session.timeout.ms": 45000,
    })
    consumer.subscribe([cfg.topic])

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("consumer started", extra={"ctx": {"topic": cfg.topic, "group": cfg.consumer_group, "batch": cfg.batch_size}})

    last_data = None
    last_report = time.monotonic()
    try:
        while running:
            msgs = consumer.consume(num_messages=cfg.batch_size, timeout=cfg.poll_timeout_s)
            raws = []
            for m in msgs:
                if m.error():
                    if m.error().code() != KafkaError._PARTITION_EOF:
                        log.error("kafka error", extra={"ctx": {"error": str(m.error())}})
                    continue
                raws.append(RawMessage(m.value(), m.key(), m.topic(), m.partition(), m.offset(), m.timestamp()[1]))
            if raws:
                last_data = time.monotonic()
                proc.process_batch(raws)
                dlq.flush(10)  # DLQ records durable BEFORE we commit past them
                consumer.commit(asynchronous=False)
            elif args.idle_exit_seconds and last_data and time.monotonic() - last_data > args.idle_exit_seconds:
                break
            if time.monotonic() - last_report > 10:
                last_report = time.monotonic()
                log.info("progress", extra={"ctx": {**vars(proc.stats), **proc.latency.summary()}})
    finally:
        proc.close()
        consumer.close()
        dlq.flush(10)
        st = proc.stats
        elapsed = (st.last_ts - st.first_ts) if st.first_ts and st.last_ts else 0
        summary = {
            "host": socket.gethostname(), "consumed": st.consumed, "written": st.written,
            "duplicates": st.duplicates, "sampled_out": st.sampled_out, "dlq": st.dlq, "decisions": st.decisions,
            "elapsed_s": elapsed, "events_per_sec": (st.consumed / elapsed) if elapsed > 0 else None,
            **proc.latency.summary(),
        }
        os.makedirs(cfg.results_dir, exist_ok=True)
        with open(os.path.join(cfg.results_dir, f"consumer_stats_{socket.gethostname()}.json"), "w") as f:
            json.dump(summary, f, indent=2)
        log.info("consumer stopped", extra={"ctx": summary})


if __name__ == "__main__":
    main()
