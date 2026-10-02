from __future__ import annotations

import json
import threading

import numpy as np
import xgboost as xgb

from .features import FEATURE_COLS


class ModelBundle:
    """Model is loaded ONCE per worker process; predict uses inplace_predict (no DMatrix build)."""

    def __init__(self, booster: xgb.Booster, meta: dict):
        if meta["feature_cols"] != FEATURE_COLS:  # guard against train/serve feature skew
            raise ValueError(f"model features {meta['feature_cols']} != code features {FEATURE_COLS}")
        self.booster, self.meta = booster, meta
        self.version: str = meta["model_version"]
        self.review_thr: float = meta["review_threshold"]
        self.block_thr: float = meta["block_threshold"]
        self._lock = threading.Lock()

    @classmethod
    def load(cls, model_path: str, meta_path: str, nthread: int = 1) -> "ModelBundle":
        booster = xgb.Booster()
        booster.load_model(model_path)
        booster.set_param({"nthread": nthread})
        with open(meta_path) as f:
            return cls(booster, json.load(f))

    def vector(self, features: dict[str, float]) -> np.ndarray:
        try:
            return np.fromiter((features[c] for c in FEATURE_COLS), dtype=np.float32, count=len(FEATURE_COLS))
        except KeyError as e:
            raise ValueError(f"missing feature {e}") from None

    def predict(self, X: np.ndarray) -> np.ndarray:
        with self._lock:
            return self.booster.inplace_predict(np.asarray(X, dtype=np.float32))

    def decide(self, score: float) -> str:
        if score >= self.block_thr:
            return "block"
        if score >= self.review_thr:
            return "review"
        return "approve"
