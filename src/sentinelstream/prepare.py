"""Build replayable events + offline features.

Real data (recommended):
    kaggle competitions download -c ieee-fraud-detection -p data/raw && unzip data/raw/*.zip -d data/raw
    python -m sentinelstream.prepare --raw-dir data/raw
Smoke test data:
    python -m sentinelstream.prepare --synthetic
"""
from __future__ import annotations

import argparse
import os

import pandas as pd

from .features import compute_offline
from .synth import make_synthetic


def load_ieee(raw_dir: str, use_identity: bool = True) -> pd.DataFrame:
    tx = pd.read_csv(
        os.path.join(raw_dir, "train_transaction.csv"),
        usecols=["TransactionID", "isFraud", "TransactionDT", "TransactionAmt", "ProductCD", "card1", "addr1"],
    )
    ident_path = os.path.join(raw_dir, "train_identity.csv")
    if use_identity and os.path.exists(ident_path):
        ident = pd.read_csv(ident_path, usecols=["TransactionID", "DeviceInfo"])
        tx = tx.merge(ident, how="left", on="TransactionID")
    else:
        tx["DeviceInfo"] = None
    device = tx["DeviceInfo"].astype(object)
    device = device.where(device.notna(), None)
    geo = tx["addr1"].map(lambda v: None if pd.isna(v) else str(int(v))).astype(object)
    df = pd.DataFrame(
        {
            "event_id": tx["TransactionID"].astype(str),
            "_id": tx["TransactionID"],
            "user_id": tx["card1"].astype(str),  # card1 is a card-level pseudo user
            "ts": tx["TransactionDT"].astype(float),
            "amount": tx["TransactionAmt"].astype(float),
            "product_cd": tx["ProductCD"].fillna("").astype(str),
            "device_id": device,
            "geo_id": geo,
            "label": tx["isFraud"].astype(int),
        }
    )
    return df.sort_values(["ts", "_id"], kind="stable").drop(columns="_id").reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--out-dir", default="data/processed")
    ap.add_argument("--synthetic", action="store_true")
    ap.add_argument("--no-identity", action="store_true")
    a = ap.parse_args()

    df = make_synthetic() if a.synthetic else load_ieee(a.raw_dir, use_identity=not a.no_identity)
    os.makedirs(a.out_dir, exist_ok=True)
    df.to_parquet(os.path.join(a.out_dir, "events.parquet"), index=False)
    feats = compute_offline(df)
    feats.to_parquet(os.path.join(a.out_dir, "features.parquet"), index=False)
    print(
        f"rows={len(df)} fraud_rate={df['label'].mean():.4f} users={df['user_id'].nunique()} "
        f"device_coverage={df['device_id'].notna().mean():.2%} synthetic={a.synthetic}"
    )


if __name__ == "__main__":
    main()
