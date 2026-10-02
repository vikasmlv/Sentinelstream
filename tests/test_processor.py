import fakeredis
import pytest
from moto import mock_aws

from sentinelstream.config import Settings
from sentinelstream.decision_store import DecisionStore, create_table
from sentinelstream.dedupe import DoneCache
from sentinelstream.feature_store import FeatureStore
from sentinelstream.processor import Processor, RawMessage
from sentinelstream.retry import PermanentError

CFG = Settings(max_retries=3, retry_base_delay_s=0.0, ddb_workers=4)


class FakeScorer:
    def __init__(self, fail_times=0, permanent=False):
        self.calls, self.fail_times, self.permanent = 0, fail_times, permanent

    def score_batch(self, items):
        self.calls += 1
        if self.permanent:
            raise PermanentError("bad")
        if self.calls <= self.fail_times:
            raise ConnectionError("scorer down")
        return [{"event_id": i, "score": 0.9 if f["amount"] > 1000 else 0.1,
                 "decision": "block" if f["amount"] > 1000 else "approve", "model_version": "t"} for i, f in items]


def msg(eid, amount=10.0, user="u1", ts=1000.0, offset=0):
    body = f'{{"event_id":"{eid}","user_id":"{user}","ts":{ts},"amount":{amount},"product_cd":"W"}}'
    return RawMessage(body.encode(), user.encode(), "transactions", 0, offset, 1_700_000_000_000)


@pytest.fixture
def env():
    with mock_aws():
        create_table("t", "us-east-1", None, 5, 5)
        r = fakeredis.FakeRedis()
        store = DecisionStore("t", "us-east-1")
        dlq = []
        make = lambda scorer: Processor(CFG, FeatureStore(r), DoneCache(r), scorer, store, lambda m, why, d: dlq.append((m, why)))
        yield make, store, dlq, r


def test_happy_path_writes_decisions(env):
    make, store, dlq, _ = env
    p = make(FakeScorer())
    p.process_batch([msg("e1"), msg("e2", amount=5000, ts=1001.0)])
    assert store.get("e1")["decision"] == "approve"
    assert store.get("e2")["decision"] == "block"
    assert p.stats.written == 2 and not dlq
    p.close()


def test_duplicates_in_batch_and_redelivery_yield_one_decision(env):
    make, store, dlq, r = env
    p = make(FakeScorer())
    p.process_batch([msg("e1"), msg("e1")])  # duplicate inside one batch
    p.process_batch([msg("e1")])  # redelivery after commit failure -> Redis fast path
    r.flushall()  # cache lost -> DynamoDB conditional put must still dedupe
    p.process_batch([msg("e1")])
    assert p.stats.written == 1
    assert p.stats.duplicates == 3
    p.close()


def test_poison_goes_to_dlq_without_blocking_good_messages(env):
    make, store, dlq, _ = env
    p = make(FakeScorer())
    p.process_batch([RawMessage(b"{not json", b"x", "transactions", 0, 1), msg("good", offset=2)])
    assert [w for _, w in dlq] == ["poison:invalid_payload"]
    assert store.get("good") is not None
    p.close()


def test_transient_scorer_failure_is_retried(env):
    make, store, dlq, _ = env
    s = FakeScorer(fail_times=2)  # succeeds on 3rd attempt == max_retries
    p = make(s)
    p.process_batch([msg("e1")])
    assert s.calls == 3 and store.get("e1") and not dlq
    p.close()


def test_exhausted_retries_and_permanent_errors_go_to_dlq(env):
    make, store, dlq, _ = env
    p = make(FakeScorer(fail_times=99))
    p.process_batch([msg("e1")])
    assert dlq[-1][1] == "score_failed" and store.get("e1") is None
    p2 = make(FakeScorer(permanent=True))
    s = p2.scorer
    p2.process_batch([msg("e2")])
    assert s.calls == 1 and dlq[-1][1] == "score_failed"  # no retries on PermanentError
    p.close(); p2.close()


def test_approve_sampling_is_deterministic(env):
    make, store, dlq, _ = env
    cfg = Settings(max_retries=1, retry_base_delay_s=0.0, ddb_workers=2, approve_sample_rate=0.0)
    r = fakeredis.FakeRedis()
    p = Processor(cfg, FeatureStore(r), DoneCache(r), FakeScorer(), store, lambda *a: None)
    p.process_batch([msg("a1"), msg("b1", amount=5000, ts=1001.0)])
    assert store.get("a1") is None and store.get("b1") is not None  # approve dropped, block kept
    assert p.stats.sampled_out == 1
    p.close()
