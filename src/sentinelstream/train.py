"""Train XGBoost and compare against the rule baseline on the SAME chronological test split.

Split (by time order): train 64% | val 16% | test 20%.
 - XGBoost fits on train, early-stops on val.
 - Operating thresholds (rule thresholds and model score thresholds) are chosen on val only.
 - Test is touched once, to report precision/recall at those frozen operating points.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import average_precision_score, roc_auc_score

from . import baseline
from .features import FEATURE_COLS
from .metrics import precision_recall, recall_at_precision, threshold_for_precision

SWEEP = (0.05, 0.10, 0.20, 0.30, 0.50)


def split(df: pd.DataFrame):
    n = len(df)
    a, b = int(n * 0.64), int(n * 0.80)
    return df.iloc[:a], df.iloc[a:b], df.iloc[b:]


def train_model(feats: pd.DataFrame, target_precision: float, out_dir: str, results_dir: str, seed: int = 42) -> dict:
    tr, va, te = split(feats)
    Xtr, ytr = tr[FEATURE_COLS].to_numpy(np.float32), tr["label"].to_numpy()
    Xva, yva = va[FEATURE_COLS].to_numpy(np.float32), va["label"].to_numpy()
    Xte, yte = te[FEATURE_COLS].to_numpy(np.float32), te["label"].to_numpy()
    if min(ytr.sum(), yva.sum(), yte.sum()) == 0:
        raise ValueError("a split has zero fraud rows; need more data")

    spw = float((ytr == 0).sum() / max(1, (ytr == 1).sum()))
    clf = xgb.XGBClassifier(
        n_estimators=600, learning_rate=0.05, max_depth=6, subsample=0.8, colsample_bytree=0.8,
        scale_pos_weight=spw, tree_method="hist", eval_metric="aucpr", early_stopping_rounds=30,
        n_jobs=os.cpu_count() or 1, random_state=seed,
    )
    clf.fit(Xtr, ytr, eval_set=[(Xva, yva)], verbose=False)
    s_va, s_te = clf.predict_proba(Xva)[:, 1], clf.predict_proba(Xte)[:, 1]

    rows = []
    for tp in SWEEP:
        rule, rule_val = baseline.tune(va, tp)
        rp, rr = precision_recall(yte, baseline.predict(te, rule))
        thr = threshold_for_precision(yva, s_va, tp)
        if thr is None:
            xp, xr, x_curve = None, None, None
        else:
            xp, xr = precision_recall(yte, s_te >= thr)
            x_curve = recall_at_precision(yte, s_te, tp)[0]
        rows.append({
            "target_precision": tp,
            "rule": {"config": rule.to_json(), "val_reached_target": rule_val["reached_target"],
                     "test_precision": rp, "test_recall": rr},
            "xgb": {"val_threshold": thr, "test_precision": xp, "test_recall": xr,
                    "test_recall_oracle_curve": x_curve},
        })

    # decision thresholds served online: review at target precision, block at precision 0.5
    review = threshold_for_precision(yva, s_va, target_precision)
    if review is None:
        review = float(np.quantile(s_va, 0.99))
    block = threshold_for_precision(yva, s_va, 0.5)
    block = max(block, review) if block is not None else 1.01

    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)
    model_path = os.path.join(out_dir, "model.json")
    clf.get_booster().save_model(model_path)
    with open(model_path, "rb") as f:
        version = "xgb-" + hashlib.sha1(f.read()).hexdigest()[:8]
    meta = {
        "model_version": version, "feature_cols": FEATURE_COLS, "review_threshold": review,
        "block_threshold": block, "target_precision": target_precision, "best_iteration": int(clf.best_iteration),
        "trained_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(os.path.join(out_dir, "model_meta.json"), "w") as f:
        json.dump(meta, f, indent=2)

    report = {
        "split_rows": {"train": len(tr), "val": len(va), "test": len(te)},
        "test_fraud_rate": float(yte.mean()),
        "xgb_test_roc_auc": float(roc_auc_score(yte, s_te)),
        "xgb_test_pr_auc": float(average_precision_score(yte, s_te)),
        "headline_target_precision": target_precision,
        "sweep": rows, "model_version": version,
    }
    with open(os.path.join(results_dir, "model_comparison.json"), "w") as f:
        json.dump(report, f, indent=2)
    with open(os.path.join(results_dir, "model_comparison.md"), "w") as f:
        f.write(render_markdown(report))
    return report


def _fmt(v) -> str:
    return "n/a" if v is None else f"{v:.3f}"


def render_markdown(rep: dict) -> str:
    lines = [
        f"Test rows: {rep['split_rows']['test']}, fraud rate {rep['test_fraud_rate']:.4f}, "
        f"XGB ROC-AUC {rep['xgb_test_roc_auc']:.3f}, PR-AUC {rep['xgb_test_pr_auc']:.3f}",
        "",
        "| target precision | rule P | rule recall | xgb P | xgb recall | abs diff (pp) | rel improvement |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rep["sweep"]:
        rr, xr = r["rule"]["test_recall"], r["xgb"]["test_recall"]
        note = "" if r["rule"]["val_reached_target"] else "*"
        if xr is None:
            diff = rel = "n/a"
        else:
            diff = f"{(xr - rr) * 100:+.1f}"
            rel = f"{(xr - rr) / rr * 100:+.1f}%" if rr > 0 else "n/a"
        lines.append(
            f"| {r['target_precision']:.2f} | {_fmt(r['rule']['test_precision'])}{note} | {_fmt(rr)} | "
            f"{_fmt(r['xgb']['test_precision'])} | {_fmt(xr)} | {diff} | {rel} |"
        )
    lines += ["", "`*` = rule baseline could not reach the target precision on val; best-F1 rule shown.",
              "Operating points frozen on val, evaluated once on test."]
    return "\n".join(lines) + "\n"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", default="data/processed/features.parquet")
    ap.add_argument("--out-dir", default="models")
    ap.add_argument("--results-dir", default="results")
    ap.add_argument("--target-precision", type=float, default=0.10,
                    help="choose BEFORE looking at results to avoid cherry-picking")
    a = ap.parse_args()
    rep = train_model(pd.read_parquet(a.features), a.target_precision, a.out_dir, a.results_dir)
    print(render_markdown(rep))
    print("saved model:", rep["model_version"])


if __name__ == "__main__":
    main()
