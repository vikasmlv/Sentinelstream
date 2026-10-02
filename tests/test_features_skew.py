"""The most important test: offline (training) features == online (Redis) features."""
import fakeredis
import numpy as np

from sentinelstream.feature_store import FeatureStore
from sentinelstream.features import FEATURE_COLS, compute_offline
from sentinelstream.schemas import Event


def _events(df):
    return [Event(**{k: (None if v != v else v) if isinstance(v, float) else v for k, v in r.items()})
            for r in df.to_dict("records")]


def test_online_matches_offline_including_timestamp_ties(synthetic_df):
    df = synthetic_df.iloc[:3000].reset_index(drop=True)
    assert df["ts"].duplicated().any(), "test data should contain timestamp ties"
    offline = compute_offline(df)

    fs = FeatureStore(fakeredis.FakeRedis())
    evs = _events(df)
    online = []
    for i in range(0, len(evs), 97):  # odd batch size on purpose
        online += fs.update_and_get(evs[i : i + 97])

    got = np.array([[f[c] for c in FEATURE_COLS] for f in online])
    want = offline[FEATURE_COLS].to_numpy()
    np.testing.assert_array_equal(got, want)


def test_replayed_event_does_not_inflate_own_counts():
    fs = FeatureStore(fakeredis.FakeRedis())
    e = Event(event_id="a", user_id="u", ts=100.0, amount=5.0, product_cd="W", device_id="d", geo_id="g")
    first = fs.update_and_get([e])[0]
    again = fs.update_and_get([e])[0]  # redelivery: ZADD of same member is a no-op
    assert first == again
    assert first["cnt_1m"] == 1.0
