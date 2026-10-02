"""Merge Locust CSV + consumer stats into results/summary.md. Every number in the resume must trace back here.
    python scripts/summarize_results.py --locust results/locust_stats.csv --label "local docker, 4 vCPU"
"""
import argparse
import csv
import glob
import json
import multiprocessing
import platform


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--locust", default="results/locust_stats.csv")
    ap.add_argument("--consumer-glob", default="results/consumer_stats_*.json")
    ap.add_argument("--label", default="unlabelled environment", help="e.g. 'EC2 t3.small' or 'MacBook M2, Docker'")
    ap.add_argument("--out", default="results/summary.md")
    a = ap.parse_args()
    lines = [f"# Measured results ({a.label})", "",
             f"Machine: {platform.platform()}, {multiprocessing.cpu_count()} logical CPUs, Python {platform.python_version()}", ""]
    try:
        with open(a.locust) as f:
            agg = next(r for r in csv.DictReader(f) if r["Name"] == "Aggregated")
        lines += ["## Scoring service (/score, Locust)", "",
                  "| requests | failures | RPS | p50 ms | p95 ms | p99 ms | max ms |", "|---|---|---|---|---|---|---|",
                  f"| {agg['Request Count']} | {agg['Failure Count']} | {float(agg['Requests/s']):.0f} | "
                  f"{agg['50%']} | {agg['95%']} | {agg['99%']} | {agg['Max Response Time']} |", ""]
    except (FileNotFoundError, StopIteration):
        lines += ["(no Locust CSV found)", ""]
    for path in sorted(glob.glob(a.consumer_glob)):
        s = json.load(open(path))
        c, k = s.get("consume_to_decision_ms", {}), s.get("kafka_to_decision_ms", {})
        lines += [f"## Consumer {s['host']}", "",
                  f"consumed={s['consumed']} written={s['written']} duplicates={s['duplicates']} dlq={s['dlq']} "
                  f"events/sec={s['events_per_sec'] and round(s['events_per_sec'])}", "",
                  f"consume->decision ms: p50={c.get('p50')} p95={c.get('p95')} p99={c.get('p99')}",
                  f"kafka->decision ms (incl. queueing): p50={k.get('p50')} p95={k.get('p95')} p99={k.get('p99')}", ""]
    open(a.out, "w").write("\n".join(lines))
    print("\n".join(lines))


if __name__ == "__main__":
    main()
