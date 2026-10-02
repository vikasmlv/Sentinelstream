import asyncio

import numpy as np
from fastapi.testclient import TestClient

from sentinelstream.batching import MicroBatcher
from sentinelstream.config import Settings
from sentinelstream.features import FEATURE_COLS
from sentinelstream.service import create_app

FEATS = dict(zip(FEATURE_COLS, [50.0, 0, 12, 1, 1, 1, 0, 1]))


def client(trained_model):
    m, meta = trained_model
    return TestClient(create_app(Settings(model_path=m, meta_path=meta, log_level="WARNING")))


def test_score_and_batch_agree(trained_model):
    with client(trained_model) as c:
        single = c.post("/score", json={"event_id": "x", "features": FEATS}).json()
        batch = c.post("/score_batch", json={"items": [{"event_id": "x", "features": FEATS}]}).json()["results"][0]
        assert single["decision"] in {"approve", "review", "block"}
        assert abs(single["score"] - batch["score"]) < 1e-6
        assert c.get("/health").json()["status"] == "ok"


def test_missing_feature_is_422(trained_model):
    with client(trained_model) as c:
        bad = {k: v for k, v in FEATS.items() if k != "cnt_1m"}
        assert c.post("/score", json={"event_id": "x", "features": bad}).status_code == 422


def test_microbatcher_groups_concurrent_requests():
    calls = []

    def predict(X):
        calls.append(len(X))
        return X[:, 0] * 2

    async def run():
        b = MicroBatcher(predict, max_size=16, max_wait_ms=20)
        await b.start()
        res = await asyncio.gather(*[b.submit(np.array([float(i)], dtype=np.float32)) for i in range(10)])
        await b.stop()
        return res

    res = asyncio.run(run())
    assert res == [float(i * 2) for i in range(10)]
    assert max(calls) > 1  # batching actually happened
