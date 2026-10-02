"""Locust load test for the scoring service.

Closed-loop (constant(0)) users => throughput is limited by the system, not by think time.
  Fixed users : locust -f loadtest/locustfile.py --host http://localhost:8080 --headless -u 100 -r 50 -t 60s --csv results/locust
  Step ramp   : STEP_LOAD=1 locust -f loadtest/locustfile.py --host http://localhost:8080 --headless --csv results/locust_step
  More CPU    : add --processes 4   (one Locust process saturates ~1 core; the generator can be the bottleneck!)
Feature vectors are sampled from the real offline feature table when present, else synthetic.
"""
import itertools
import json
import os
import random

from locust import FastHttpUser, LoadTestShape, constant, task

from sentinelstream.features import FEATURE_COLS


def _pool() -> list[str]:
    path = os.getenv("LOAD_FEATURES", "data/processed/features.parquet")
    if os.path.exists(path):
        import pandas as pd

        rows = pd.read_parquet(path)[FEATURE_COLS].sample(20_000, random_state=0, replace=True).to_dict("records")
    else:
        rnd = random.Random(0)
        rows = [dict(zip(FEATURE_COLS, [rnd.lognormvariate(3.5, 1), rnd.randint(0, 4), rnd.randint(0, 23),
                                        rnd.randint(1, 6), rnd.randint(1, 20), rnd.randint(1, 60),
                                        rnd.randint(0, 10), rnd.randint(0, 30)])) for _ in range(20_000)]
    return [json.dumps(r) for r in rows]


POOL = _pool()
_ids = itertools.count()
HEADERS = {"Content-Type": "application/json"}


class ScoreUser(FastHttpUser):
    wait_time = constant(0)

    @task
    def score(self):
        body = '{"event_id":"lt-%d","features":%s}' % (next(_ids), random.choice(POOL))
        with self.client.post("/score", data=body, headers=HEADERS, name="/score", catch_response=True) as r:
            if r.status_code != 200:
                r.failure(f"status {r.status_code}")


if os.getenv("STEP_LOAD") == "1":
    STAGES = [int(x) for x in os.getenv("STEP_USERS", "10,25,50,100,200,400").split(",")]
    STEP_SECONDS = int(os.getenv("STEP_SECONDS", "45"))

    class StepShape(LoadTestShape):
        """Hold each user level for STEP_SECONDS; find the knee where p99 explodes."""

        def tick(self):
            i = int(self.get_run_time() // STEP_SECONDS)
            if i >= len(STAGES):
                return None
            return STAGES[i], STAGES[i]
