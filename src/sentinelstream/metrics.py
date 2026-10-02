from __future__ import annotations

import numpy as np
from sklearn.metrics import precision_recall_curve


def precision_recall(y: np.ndarray, pred: np.ndarray) -> tuple[float, float]:
    y = np.asarray(y).astype(bool)
    pred = np.asarray(pred).astype(bool)
    tp = int(np.sum(y & pred))
    p = tp / max(1, int(pred.sum()))
    r = tp / max(1, int(y.sum()))
    return p, r


def recall_at_precision(y: np.ndarray, score: np.ndarray, target: float) -> tuple[float, float | None]:
    """Max recall over thresholds whose precision >= target. Returns (recall, threshold);
    (0.0, None) if the target precision is unreachable."""
    prec, rec, thr = precision_recall_curve(y, score)
    mask = prec[:-1] >= target
    if not mask.any():
        return 0.0, None
    i = int(np.argmax(np.where(mask, rec[:-1], -1.0)))
    return float(rec[i]), float(thr[i])


def threshold_for_precision(y: np.ndarray, score: np.ndarray, target: float) -> float | None:
    return recall_at_precision(y, score, target)[1]
