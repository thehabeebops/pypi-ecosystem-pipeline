"""Discovery crawl: start from seed packages and follow dependencies breadth-first.

Properties: bounded (depth and package budget), de-duplicated, resumable (state is checkpointed),
incremental (fresh packages are skipped, stale ones use conditional requests), and raw snapshots
are immutable and content-addressed by PyPI's serial number.
"""
import datetime as dt
import gzip
import json
import re
from collections import Counter, deque
from pathlib import Path

from .client import FetchError, PyPIClient

_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")


def normalize(name: str) -> str:
    """PEP 503 name normalisation."""
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_requirement(req: str) -> tuple[str, str, str, bool]:
    """'PySocks!=1.5.7,>=1.5.6; extra == "socks"' -> (name, specifier, marker, is_optional)."""
    main, _, marker = req.partition(";")
    m = _NAME.match(main)
    name = normalize(m.group(1)) if m else ""
    rest = main[m.end():] if m else ""
    spec = re.sub(r"^\s*\[[^\]]*\]", "", rest).strip().strip("()").strip()
    return name, spec, marker.strip(), bool(re.search(r"extra\s*==", marker))


def _utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def load_snapshot(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def _store(path: Path, doc: dict) -> None:
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))
    tmp.replace(path)  # atomic: a crash never leaves a half-written snapshot


def crawl(root, client: PyPIClient, seeds, max_packages=300, max_depth=3, max_age_hours=24.0, force=False, now=_utcnow) -> dict:
    root = Path(root)
    raw = root / "bronze" / "pypi"
    raw.mkdir(parents=True, exist_ok=True)
    state_path = root / "audit" / "crawl_state.json"
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state: dict = json.loads(state_path.read_text()) if state_path.exists() else {}
    stats: Counter = Counter()
    queue: deque = deque()
    seen: set = set()
    for s in seeds:
        n = normalize(s)
        if n and n not in seen and len(seen) < max_packages:
            seen.add(n)
            queue.append((n, 0))

    def save() -> None:
        state_path.write_text(json.dumps(state, indent=1, sort_keys=True))

    processed = 0
    while queue:
        name, depth = queue.popleft()
        st = state.get(name)
        ok = bool(st and st.get("status") == "ok")
        ts = now().isoformat(timespec="seconds")
        fresh = ok and not force and now() - dt.datetime.fromisoformat(st["fetched_at"]) < dt.timedelta(hours=max_age_hours)
        if fresh:
            stats["skipped_fresh"] += 1
            doc = load_snapshot(root / st["path"])
        else:
            try:
                resp = client.fetch(name, etag=st.get("etag") if ok else None)
            except FetchError as exc:
                stats["errors"] += 1
                state[name] = {"status": "error", "error": str(exc), "fetched_at": ts}
                continue
            if resp.status == 404:
                stats["not_found"] += 1
                state[name] = {"status": "not_found", "fetched_at": ts}
                continue
            if resp.status == 304:
                stats["not_modified"] += 1
                st["fetched_at"] = ts
                doc = load_snapshot(root / st["path"])
            else:
                doc = resp.body
                serial = doc["last_serial"]
                path = raw / f"{name}__{serial}.json.gz"
                if path.exists():
                    stats["unchanged_serial"] += 1
                else:
                    _store(path, doc)
                    stats["fetched"] += 1
                state[name] = {"status": "ok", "etag": resp.etag, "serial": serial, "fetched_at": ts,
                               "path": str(path.relative_to(root)), "depth": depth}
        if depth < max_depth:
            deps = {r[0] for r in map(parse_requirement, doc["info"].get("requires_dist") or []) if r[0] and not r[3]}
            for d in sorted(deps):
                if d not in seen and len(seen) < max_packages:
                    seen.add(d)
                    queue.append((d, depth + 1))
        processed += 1
        if processed % 25 == 0:
            save()  # checkpoint: a crash resumes from here
    save()
    stats["discovered"] = len(seen)
    stats["packages_ok"] = sum(1 for n in seen if state.get(n, {}).get("status") == "ok")
    return dict(stats)
