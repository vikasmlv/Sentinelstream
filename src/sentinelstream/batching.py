"""Opportunistic micro-batching for /score.

One worker task drains the queue and runs ONE predict for everything waiting.
 - max_wait_ms = 0 : never delays a request; batches form naturally under load
   (while a predict runs, new requests queue up). Best p99 at low load.
 - max_wait_ms > 0 : waits up to that long to fill a batch. Higher throughput per CPU
   at the cost of added latency; only useful if you are CPU-bound in predict.
"""
from __future__ import annotations

import asyncio
from typing import Callable

import numpy as np


class MicroBatcher:
    def __init__(self, predict_fn: Callable[[np.ndarray], np.ndarray], max_size: int = 64, max_wait_ms: float = 0.0):
        self.predict_fn = predict_fn
        self.max_size = max_size
        self.max_wait_s = max_wait_ms / 1000.0
        self._queue: asyncio.Queue | None = None
        self._task: asyncio.Task | None = None
        self.batches = 0
        self.items = 0

    async def start(self) -> None:
        self._queue = asyncio.Queue()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def submit(self, row: np.ndarray) -> float:
        fut = asyncio.get_running_loop().create_future()
        self._queue.put_nowait((row, fut))
        return await fut

    def stats(self) -> dict:
        return {"batches": self.batches, "items": self.items,
                "avg_batch_size": (self.items / self.batches) if self.batches else 0.0}

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            batch = [await self._queue.get()]
            if self.max_wait_s > 0:
                deadline = loop.time() + self.max_wait_s
                while len(batch) < self.max_size:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        break
                    try:
                        batch.append(await asyncio.wait_for(self._queue.get(), remaining))
                    except asyncio.TimeoutError:
                        break
            else:
                while len(batch) < self.max_size:
                    try:
                        batch.append(self._queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break
            X = np.stack([r for r, _ in batch])
            self.batches += 1
            self.items += len(batch)
            try:
                preds = await loop.run_in_executor(None, self.predict_fn, X)
                for (_, fut), p in zip(batch, preds):
                    if not fut.done():
                        fut.set_result(float(p))
            except Exception as exc:  # noqa: BLE001
                for _, fut in batch:
                    if not fut.done():
                        fut.set_exception(exc)
