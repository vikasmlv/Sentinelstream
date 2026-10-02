"""Synthetic transactions for SMOKE TESTS ONLY. Numbers from this data must never go on a resume."""
from __future__ import annotations

import numpy as np
import pandas as pd


def make_synthetic(n: int = 60_000, n_users: int = 3_000, seed: int = 0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    horizon = 14 * 86400
    ts = np.sort(rng.integers(0, horizon, size=n)).astype(float)
    users = rng.zipf(1.6, size=n) % n_users
    amount = np.round(rng.lognormal(3.5, 1.0, size=n), 2)
    label = np.zeros(n, dtype=int)
    device = np.array([f"dev{u % 1200}" for u in users], dtype=object)
    device[rng.random(n) < 0.35] = None
    geo = np.array([str(100 + (u % 40)) for u in users], dtype=object)
    product = rng.choice(list("WCRHS"), size=n, p=[0.6, 0.15, 0.1, 0.1, 0.05])

    # inject fraud bursts: many quick txns, higher amounts, shared device
    n_bursts = max(5, n // 400)
    for b in range(n_bursts):
        start = int(rng.integers(0, n - 30))
        length = int(rng.integers(6, 20))
        idx = np.arange(start, min(n, start + length))
        u = int(rng.integers(0, n_users))
        users[idx] = u
        ts[idx] = ts[start] + np.sort(rng.integers(0, 120, size=len(idx)))
        amount[idx] = np.round(rng.lognormal(5.0, 0.6, size=len(idx)), 2)
        device[idx] = f"fraud_dev{b % 7}"
        label[idx] = 1
    # a little label noise: some isolated large fraud
    solo = rng.choice(n, size=n // 300, replace=False)
    label[solo] = 1
    amount[solo] *= 4

    df = pd.DataFrame(
        {
            "event_id": np.arange(1, n + 1).astype(str),
            "_id": np.arange(1, n + 1),
            "user_id": users.astype(str),
            "ts": ts,
            "amount": amount,
            "product_cd": product,
            "device_id": device,
            "geo_id": geo,
            "label": label,
        }
    )
    df = df.sort_values(["ts", "_id"], kind="stable").drop(columns="_id").reset_index(drop=True)
    return df
