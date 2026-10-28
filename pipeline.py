"""DAG runner: dependency order, retries with backoff, per-task audit trail, quality gate before marts."""
import datetime as dt
import json
import time
import uuid
from dataclasses import dataclass
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Callable

import duckdb

from . import crawl as crawl_mod
from . import gold, quality, report, silver
from .client import PyPIClient


class PipelineFailed(RuntimeError):
    """A critical data-quality gate failed. Never retried."""


@dataclass
class Task:
    name: str
    fn: Callable[[], dict]
    deps: tuple[str, ...] = ()


def _append(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record, default=str) + "\n")


def run(root, seeds, client: PyPIClient | None = None, max_packages=300, max_depth=3, max_age_hours=24.0,
        force=False, retries=1, now=crawl_mod._utcnow) -> dict:
    root = Path(root)
    con = duckdb.connect()
    client = client or PyPIClient()
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]
    audit = root / "audit"
    ctx: dict = {}

    def do_crawl() -> dict:
        ctx["crawl"] = crawl_mod.crawl(root, client, seeds, max_packages, max_depth, max_age_hours, force, now)
        return {**ctx["crawl"], "http": dict(client.stats)}

    def do_silver() -> dict:
        ctx["silver"] = silver.build(root, con)
        return ctx["silver"]

    def gate() -> dict:
        results = quality.run_checks(root, con, ctx["crawl"], ctx["silver"]["as_of"])
        for r in results:
            _append(audit / "dq_results.jsonl", {"run_id": run_id, **r})
        failed = [r["check"] for r in results if r["severity"] == "critical" and not r["passed"]]
        if failed:
            raise PipelineFailed("critical data-quality checks failed: " + ", ".join(failed))
        return {"checks": len(results), "warnings": sum(1 for r in results if not r["passed"])}

    tasks = {
        "crawl": Task("crawl", do_crawl),
        "silver": Task("silver", do_silver, ("crawl",)),
        "quality": Task("quality", gate, ("silver",)),
        "marts": Task("marts", lambda: gold.build_marts(root, con, ctx["silver"]["as_of"]), ("quality",)),
        "report": Task("report", lambda: report.build(root, con, ctx["silver"]["as_of"]), ("marts",)),
    }
    results: dict = {}
    for name in TopologicalSorter({n: t.deps for n, t in tasks.items()}).static_order():
        started = time.time()
        for attempt in range(1, retries + 2):
            try:
                metrics = tasks[name].fn()
                _append(audit / "run_log.jsonl", {"run_id": run_id, "task": name, "status": "ok", "attempt": attempt,
                                                  "seconds": round(time.time() - started, 2), "metrics": metrics})
                results[name] = metrics
                break
            except PipelineFailed as exc:
                _append(audit / "run_log.jsonl", {"run_id": run_id, "task": name, "status": "failed", "error": str(exc)})
                raise
            except Exception as exc:
                if attempt > retries:
                    _append(audit / "run_log.jsonl", {"run_id": run_id, "task": name, "status": "failed", "error": repr(exc)})
                    raise
                time.sleep(0.5 * attempt)
    return results
