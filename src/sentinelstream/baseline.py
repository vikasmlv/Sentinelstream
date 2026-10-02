"""Rule-based baseline: flag if any velocity/amount rule fires. Thresholds are tuned on the
VALIDATION split (never on test) to maximise recall subject to precision >= target."""
from __future__ import annotations

import itertools
from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from .metrics import precision_recall

INF = float("inf")


@dataclass(frozen=True)
class RuleConfig:
    cnt_1m: float
    cnt_10m: float
    amount: float

    def to_json(self) -> dict:
        return {k: (None if v == INF else v) for k, v in asdict(self).items()}


def predict(df: pd.DataFrame, rule: RuleConfig) -> np.ndarray:
    return ((df["cnt_1m"] >= rule.cnt_1m) | (df["cnt_10m"] >= rule.cnt_10m) | (df["amount"] >= rule.amount)).to_numpy()


def grid(df_val: pd.DataFrame):
    amounts = [float(df_val["amount"].quantile(q)) for q in (0.90, 0.95, 0.98, 0.99, 0.995)] + [INF]
    for c1, c10, amt in itertools.product([2, 3, 4, 6, 10, INF], [3, 5, 8, 12, 20, INF], amounts):
        yield RuleConfig(c1, c10, amt)


def tune(df_val: pd.DataFrame, target_precision: float) -> tuple[RuleConfig, dict]:
    y = df_val["label"].to_numpy()
    best, best_key, best_m = None, None, None
    fallback, fb_f1, fb_m = None, -1.0, None
    for rule in grid(df_val):
        p, r = precision_recall(y, predict(df_val, rule))
        f1 = 0.0 if p + r == 0 else 2 * p * r / (p + r)
        if f1 > fb_f1:
            fallback, fb_f1, fb_m = rule, f1, {"precision": p, "recall": r}
        if p >= target_precision and r > 0:
            key = (r, p)
            if best_key is None or key > best_key:
                best, best_key, best_m = rule, key, {"precision": p, "recall": r}
    if best is not None:
        return best, {**best_m, "reached_target": True}
    return fallback, {**fb_m, "reached_target": False}
