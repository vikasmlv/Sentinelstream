"""Create Kafka topics and the DynamoDB table (idempotent). python -m sentinelstream.admin all"""
from __future__ import annotations

import argparse
import logging

from confluent_kafka.admin import AdminClient, NewTopic

from .config import Settings
from .decision_store import create_table
from .logging_conf import setup_logging

log = logging.getLogger("sentinel.admin")


def create_topics(cfg: Settings) -> None:
    admin = AdminClient({"bootstrap.servers": cfg.kafka_bootstrap})
    topics = [NewTopic(cfg.topic, cfg.topic_partitions, 1), NewTopic(cfg.dlq_topic, 1, 1)]
    for name, fut in admin.create_topics(topics).items():
        try:
            fut.result()
            log.info("topic created", extra={"ctx": {"topic": name}})
        except Exception as e:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" in str(e):
                log.info("topic exists", extra={"ctx": {"topic": name}})
            else:
                raise


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["topics", "table", "all"])
    ap.add_argument("--with-gsi", action="store_true", help="add user_id+ts GSI (uses extra WCU budget)")
    a = ap.parse_args()
    cfg = Settings.from_env()
    setup_logging(cfg.log_level)
    if a.what in ("topics", "all"):
        create_topics(cfg)
    if a.what in ("table", "all"):
        create_table(cfg.decisions_table, cfg.aws_region, cfg.dynamodb_endpoint, cfg.ddb_wcu, cfg.ddb_rcu, a.with_gsi)
        log.info("table ready", extra={"ctx": {"table": cfg.decisions_table}})


if __name__ == "__main__":
    main()
