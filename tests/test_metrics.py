import numpy as np

from sentinelstream.metrics import precision_recall, recall_at_precision


def test_precision_recall():
    p, r = precision_recall(np.array([1, 1, 0, 0]), np.array([1, 0, 1, 0]))
    assert (p, r) == (0.5, 0.5)


def test_recall_at_precision_reachable_and_not():
    y = np.array([0, 0, 1, 1, 0, 1])
    s = np.array([0.1, 0.2, 0.9, 0.8, 0.85, 0.3])
    rec, thr = recall_at_precision(y, s, 0.6)
    assert thr is not None and rec > 0
    rec0, thr0 = recall_at_precision(np.array([0, 0, 0, 1]), np.array([0.9, 0.8, 0.7, 0.1]), 0.9)
    assert (rec0, thr0) == (0.0, None)
