import argparse
import json
from pathlib import Path

import duckdb

from .client import PyPIClient
from .pipeline import run


def main() -> None:
    p = argparse.ArgumentParser(prog="pypipe")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="crawl PyPI, build silver and gold, run quality gates, write the report")
    r.add_argument("--root", default="data")
    r.add_argument("--seeds", default="config/seeds.txt")
    r.add_argument("--max-packages", type=int, default=300)
    r.add_argument("--max-depth", type=int, default=3)
    r.add_argument("--max-age-hours", type=float, default=24.0)
    r.add_argument("--rate", type=float, default=5.0, help="requests per second")
    r.add_argument("--force", action="store_true", help="ignore freshness and re-check every package")
    st = sub.add_parser("status", help="show the last run and its data-quality results")
    st.add_argument("--root", default="data")
    a = p.parse_args()
    if a.cmd == "run":
        seeds = [ln.strip() for ln in Path(a.seeds).read_text().splitlines() if ln.strip() and not ln.startswith("#")]
        print(json.dumps(run(a.root, seeds, PyPIClient(rate_per_s=a.rate), a.max_packages, a.max_depth, a.max_age_hours, a.force), indent=1))
    else:
        root = Path(a.root)
        log = [json.loads(x) for x in (root / "audit" / "run_log.jsonl").read_text().splitlines()]
        last = log[-1]["run_id"]
        print("Last run:", last)
        for rec in (x for x in log if x["run_id"] == last):
            print(f"  {rec['task']:<8} {rec['status']:<6} {rec.get('seconds', '-'):>7}s  {rec.get('metrics', rec.get('error'))}")
        for d in (json.loads(x) for x in (root / "audit" / "dq_results.jsonl").read_text().splitlines()):
            if d["run_id"] == last:
                print(f"  {'PASS' if d['passed'] else 'FAIL':<5} {d['severity']:<9} {d['check']:<30} {d['value']} (limit {d['threshold']})")
        con = duckdb.connect()
        print("\nMost depended-upon:", con.execute(f"SELECT package, transitive_dependents FROM read_parquet('{root}/gold/dependency_reach.parquet') LIMIT 5").fetchall())


if __name__ == "__main__":
    main()
