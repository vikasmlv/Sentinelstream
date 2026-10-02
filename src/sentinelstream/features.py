"""Single source of truth for feature definitions (offline training AND online serving).

Window semantics (identical offline and online):
    cnt_W(e) = number of events of the same key with ts in (e.ts - W, e.ts]
               that were processed up to and INCLUDING e, in replay order.
Replay order = (ts, numeric event id), fixed once in `prepare.py`.
Online this is: ZADD current event, then ZCOUNT key (ts-W ts].
`tests/test_features_skew.py` proves offline == online.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

WINDOWS_S = (60, 600, 3600)
MAX_WINDOW_S = 3600
PRODUCT_CODES = {"W": 0, "C": 1, "R": 2, "H": 3, "S": 4}
FEATURE_COLS = [
    "amount",
    "product_cd_code",
    "hour",
    "cnt_1m",
    "cnt_10m",
    "cnt_60m",
    "dev_cnt_60m",
    "geo_cnt_60m",
]


def product_code(product_cd: str | None) -> int:
    return PRODUCT_CODES.get(product_cd or "", -1)


def hour_of_day(ts: float) -> int:
    return int(ts // 3600) % 24


def static_features(amount: float, product_cd: str | None, ts: float) -> dict[str, float]:
    """Stateless features. Used online; offline path vectorises the same formulas."""
    return {
        "amount": float(amount),
        "product_cd_code": float(product_code(product_cd)),
        "hour": float(hour_of_day(ts)),
    }


def _window_counts(ts: np.ndarray, keys: pd.Series, windows: tuple[int, ...]) -> dict[int, np.ndarray]:
    """Per-key sliding counts. Rows with null key get 0 (online: key not tracked)."""
    out = {w: np.zeros(len(ts), dtype=np.int64) for w in windows}
    s = pd.Series(keys.to_numpy())
    for idx in s.groupby(s, sort=False).indices.values():  # groupby drops null keys
        t = ts[idx]  # non-decreasing because df is in replay order
        pos = np.arange(len(t))
        for w in windows:
            left = np.searchsorted(t, t - w, side="right")  # first index with ts > e.ts - w
            out[w][idx] = pos - left + 1
    return out


def compute_offline(df: pd.DataFrame) -> pd.DataFrame:
    """df columns: event_id, user_id, ts, amount, product_cd, device_id, geo_id, label.
    df MUST already be in replay order (ts, numeric id)."""
    df = df.reset_index(drop=True)
    ts = df["ts"].to_numpy(dtype=float)
    if len(ts) > 1 and np.any(np.diff(ts) < 0):
        raise ValueError("df must be sorted by ts (replay order)")
    user = _window_counts(ts, df["user_id"], WINDOWS_S)
    dev = _window_counts(ts, df["device_id"], (3600,))
    geo = _window_counts(ts, df["geo_id"], (3600,))
    out = pd.DataFrame(
        {
            "event_id": df["event_id"].astype(str),
            "label": df["label"].astype(int),
            "amount": df["amount"].astype(float),
            "product_cd_code": df["product_cd"].map(lambda p: float(product_code(p))),
            "hour": (df["ts"] // 3600 % 24).astype(float),
            "cnt_1m": user[60].astype(float),
            "cnt_10m": user[600].astype(float),
            "cnt_60m": user[3600].astype(float),
            "dev_cnt_60m": dev[3600].astype(float),
            "geo_cnt_60m": geo[3600].astype(float),
        }
    )
    return out[["event_id", "label", *FEATURE_COLS]]
