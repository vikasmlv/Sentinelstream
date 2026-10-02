import os

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="session")
def synthetic_df():
    from sentinelstream.synth import make_synthetic

    return make_synthetic(n=8000, n_users=300, seed=1)


@pytest.fixture(scope="session")
def trained_model(tmp_path_factory, synthetic_df):
    from sentinelstream.features import compute_offline
    from sentinelstream.train import train_model

    d = tmp_path_factory.mktemp("model")
    train_model(compute_offline(synthetic_df), 0.10, str(d), str(d))
    return str(d / "model.json"), str(d / "model_meta.json")
