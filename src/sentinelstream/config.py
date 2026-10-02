"""All runtime config comes from environment variables (12-factor)."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "localhost:9092"
    topic: str = "transactions"
    dlq_topic: str = "transactions.dlq"
    topic_partitions: int = 6
    consumer_group: str = "sentinel-scorers"
    redis_url: str = "redis://localhost:6379/0"
    dynamodb_endpoint: str | None = None
    aws_region: str = "us-east-1"
    decisions_table: str = "sentinel_decisions"
    ddb_wcu: int = 25
    ddb_rcu: int = 25
    scorer_url: str = "http://localhost:8080"
    model_path: str = "models/model.json"
    meta_path: str = "models/model_meta.json"
    batch_size: int = 100
    poll_timeout_s: float = 0.5
    max_retries: int = 3  # total attempts per operation
    retry_base_delay_s: float = 0.1
    dedupe_ttl_s: int = 86400
    ddb_workers: int = 16
    approve_sample_rate: float = 1.0
    xgb_nthread: int = 1
    batcher_max_size: int = 64
    batcher_max_wait_ms: float = 0.0
    log_level: str = "INFO"
    results_dir: str = "results"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap=_get("KAFKA_BOOTSTRAP", "localhost:9092"),
            topic=_get("TOPIC", "transactions"),
            dlq_topic=_get("DLQ_TOPIC", "transactions.dlq"),
            topic_partitions=int(_get("TOPIC_PARTITIONS", "6")),
            consumer_group=_get("CONSUMER_GROUP", "sentinel-scorers"),
            redis_url=_get("REDIS_URL", "redis://localhost:6379/0"),
            dynamodb_endpoint=os.environ.get("DYNAMODB_ENDPOINT") or None,
            aws_region=_get("AWS_REGION", "us-east-1"),
            decisions_table=_get("DECISIONS_TABLE", "sentinel_decisions"),
            ddb_wcu=int(_get("DDB_WCU", "25")),
            ddb_rcu=int(_get("DDB_RCU", "25")),
            scorer_url=_get("SCORER_URL", "http://localhost:8080"),
            model_path=_get("MODEL_PATH", "models/model.json"),
            meta_path=_get("META_PATH", "models/model_meta.json"),
            batch_size=int(_get("BATCH_SIZE", "100")),
            poll_timeout_s=float(_get("POLL_TIMEOUT_S", "0.5")),
            max_retries=int(_get("MAX_RETRIES", "3")),
            retry_base_delay_s=float(_get("RETRY_BASE_DELAY_S", "0.1")),
            dedupe_ttl_s=int(_get("DEDUPE_TTL_S", "86400")),
            ddb_workers=int(_get("DDB_WORKERS", "16")),
            approve_sample_rate=float(_get("APPROVE_SAMPLE_RATE", "1.0")),
            xgb_nthread=int(_get("XGB_NTHREAD", "1")),
            batcher_max_size=int(_get("BATCHER_MAX_SIZE", "64")),
            batcher_max_wait_ms=float(_get("BATCHER_MAX_WAIT_MS", "0")),
            log_level=_get("LOG_LEVEL", "INFO"),
            results_dir=_get("RESULTS_DIR", "results"),
        )
