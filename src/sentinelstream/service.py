"""FastAPI scoring service. Run: uvicorn sentinelstream.service:app --port 8080 --workers 2"""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .batching import MicroBatcher
from .config import Settings
from .features import FEATURE_COLS
from .logging_conf import setup_logging
from .scorer import ModelBundle

log = logging.getLogger("sentinel.service")


class ScoreRequest(BaseModel):
    event_id: str
    features: dict[str, float]


class ScoreBatchRequest(BaseModel):
    items: list[ScoreRequest] = Field(min_length=1, max_length=1000)


def create_app(settings: Settings | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        s = settings or Settings.from_env()
        setup_logging(s.log_level)
        bundle = ModelBundle.load(s.model_path, s.meta_path, nthread=s.xgb_nthread)
        bundle.predict(np.zeros((1, len(FEATURE_COLS)), dtype=np.float32))  # warm-up
        batcher = MicroBatcher(bundle.predict, s.batcher_max_size, s.batcher_max_wait_ms)
        await batcher.start()
        app.state.bundle, app.state.batcher = bundle, batcher
        log.info("model loaded", extra={"ctx": {"version": bundle.version}})
        yield
        await batcher.stop()

    app = FastAPI(title="SentinelStream scorer", lifespan=lifespan)

    @app.get("/health")
    async def health():
        return {"status": "ok", "model_version": app.state.bundle.version}

    @app.get("/stats")
    async def stats():
        return app.state.batcher.stats()

    @app.post("/score")
    async def score(req: ScoreRequest):
        b: ModelBundle = app.state.bundle
        try:
            row = b.vector(req.features)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        p = await app.state.batcher.submit(row)
        return {"event_id": req.event_id, "score": p, "decision": b.decide(p), "model_version": b.version}

    @app.post("/score_batch")
    async def score_batch(req: ScoreBatchRequest):
        b: ModelBundle = app.state.bundle
        try:
            X = np.stack([b.vector(i.features) for i in req.items])
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))
        preds = await asyncio.get_running_loop().run_in_executor(None, b.predict, X)
        return {"results": [
            {"event_id": i.event_id, "score": float(p), "decision": b.decide(float(p)), "model_version": b.version}
            for i, p in zip(req.items, preds)
        ]}

    return app


app = create_app()
