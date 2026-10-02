"""DynamoDB decision + audit store. PK = event_id.

Conditional put `attribute_not_exists(event_id)` makes the write idempotent: the source of
truth for dedupe. Each item is also the audit record (features snapshot, score, model version).
"""
from __future__ import annotations

import math
import os
import threading
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


def _to_ddb(obj):
    """DynamoDB rejects float; convert to Decimal, drop None/non-finite."""
    if isinstance(obj, float):
        return Decimal(str(obj)) if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: v for k, v in ((k, _to_ddb(v)) for k, v in obj.items()) if v is not None}
    if isinstance(obj, list):
        return [_to_ddb(v) for v in obj]
    return obj


def _resource(region: str, endpoint_url: str | None):
    if endpoint_url:  # DynamoDB Local accepts any credentials
        os.environ.setdefault("AWS_ACCESS_KEY_ID", "local")
        os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "local")
    return boto3.resource("dynamodb", region_name=region, endpoint_url=endpoint_url)


class DecisionStore:
    def __init__(self, table_name: str, region: str, endpoint_url: str | None = None):
        self.table_name, self.region, self.endpoint = table_name, region, endpoint_url
        self._local = threading.local()  # boto3 resources are not thread-safe

    @property
    def table(self):
        t = getattr(self._local, "table", None)
        if t is None:
            t = _resource(self.region, self.endpoint).Table(self.table_name)
            self._local.table = t
        return t

    def put_decision(self, item: dict) -> bool:
        """True if newly written, False if event_id already existed (duplicate)."""
        try:
            self.table.put_item(Item=_to_ddb(item), ConditionExpression="attribute_not_exists(event_id)")
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def get(self, event_id: str) -> dict | None:
        return self.table.get_item(Key={"event_id": event_id}).get("Item")


def create_table(name: str, region: str, endpoint_url: str | None, wcu: int, rcu: int, with_gsi: bool = False) -> None:
    """Provisioned mode (free tier = 25 WCU/25 RCU total). GSI shares that budget => off by default."""
    res = _resource(region, endpoint_url)
    kwargs = dict(
        TableName=name,
        KeySchema=[{"AttributeName": "event_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "event_id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": rcu, "WriteCapacityUnits": wcu},
    )
    if with_gsi:
        kwargs["AttributeDefinitions"] += [
            {"AttributeName": "user_id", "AttributeType": "S"},
            {"AttributeName": "ts", "AttributeType": "N"},
        ]
        kwargs["GlobalSecondaryIndexes"] = [{
            "IndexName": "user_ts",
            "KeySchema": [{"AttributeName": "user_id", "KeyType": "HASH"}, {"AttributeName": "ts", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": {"ReadCapacityUnits": max(1, rcu // 5), "WriteCapacityUnits": max(1, wcu // 5)},
        }]
    try:
        res.create_table(**kwargs)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceInUseException":
            raise
    res.Table(name).wait_until_exists()
