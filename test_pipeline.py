import datetime as dt
import json
import urllib.error

import duckdb
import pytest

from pypipe.client import FetchError, PyPIClient, RateLimiter
from pypipe.crawl import crawl, normalize, parse_requirement
from pypipe.parse import is_prerelease, parse_snapshot
from pypipe.pipeline import run

T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


def file(upload="2024-01-01T00:00:00.000000Z", ptype="bdist_wheel", yanked=False):
    return {"filename": f"x-{ptype}", "packagetype": ptype, "python_version": "py3", "size": 10,
            "upload_time_iso_8601": upload, "yanked": yanked}


def doc(name, requires=(), releases=None, serial=1, classifiers=("Programming Language :: Python :: 3.12",)):
    return {"info": {"name": name, "version": "1.0", "summary": "s", "requires_python": ">=3.8", "classifiers": list(classifiers),
                     "requires_dist": list(requires), "license_expression": "MIT", "yanked": False},
            "last_serial": serial, "releases": releases if releases is not None else {"1.0": [file()]}}


class FakePyPI:
    """Stands in for pypi.org: serves docs, supports ETags, records every call."""
    def __init__(self, docs):
        self.docs, self.calls = docs, []

    def __call__(self, url, headers, timeout):
        name = url.split("/pypi/")[1].split("/")[0]
        self.calls.append((name, headers.get("If-None-Match")))
        if name not in self.docs:
            return 404, {}, b""
        etag = f'"{name}-{self.docs[name]["last_serial"]}"'
        if headers.get("If-None-Match") == etag:
            return 304, {"ETag": etag}, b""
        return 200, {"ETag": etag}, json.dumps(self.docs[name]).encode()


def client_for(fake, **kw):
    return PyPIClient(rate_per_s=1000, sleep=lambda s: None, transport=fake, **kw)


# ---- client ---------------------------------------------------------------------
def test_retries_with_backoff_honouring_retry_after():
    seq = [(429, {"Retry-After": "2"}, b""), (503, {}, b""), (200, {"ETag": '"e"'}, b'{"last_serial": 5}')]
    sleeps = []
    c = PyPIClient(rate_per_s=1000, sleep=sleeps.append, transport=lambda *a: seq.pop(0), jitter=lambda: 0.5)
    resp = c.fetch("pkg")
    assert resp.status == 200 and c.stats["retries"] == 2 and c.stats["rate_limited"] == 1
    assert 2.0 in sleeps, "Retry-After must be honoured (2s x jitter factor 1.0)"


def test_gives_up_after_max_retries():
    c = PyPIClient(rate_per_s=1000, max_retries=2, sleep=lambda s: None, transport=lambda *a: (500, {}, b""))
    with pytest.raises(FetchError):
        c.fetch("pkg")
    assert c.stats["requests"] == 3


def test_network_errors_are_retried():
    calls = {"n": 0}

    def flaky(*a):
        calls["n"] += 1
        if calls["n"] < 3:
            raise urllib.error.URLError("boom")
        return 404, {}, b""
    assert client_for(flaky).fetch("pkg").status == 404


def test_rate_limiter_spaces_requests():
    now, sleeps = [0.0], []
    rl = RateLimiter(5, clock=lambda: now[0], sleep=lambda s: (sleeps.append(round(s, 3)), now.__setitem__(0, now[0] + s)))
    for _ in range(4):
        rl.wait()
    assert sleeps == [0.2, 0.2, 0.2]


# ---- parsing --------------------------------------------------------------------
def test_requirement_parsing():
    assert parse_requirement("Requests[socks]>=2.0,<3") == ("requests", ">=2.0,<3", "", False)
    assert parse_requirement('PySocks!=1.5.7; extra == "socks"')[3] is True
    assert parse_requirement("old-style (>=1.0)")[:2] == ("old-style", ">=1.0")
    assert normalize("Typing_Extensions.Foo") == "typing-extensions-foo"


def test_prerelease_detection():
    assert all(is_prerelease(v) for v in ("1.0.0rc1", "2.0a1", "1.2.3.dev4", "1.0b2"))
    assert not any(is_prerelease(v) for v in ("2.31.0", "1.0.0.post1", "20240101"))


def test_parse_snapshot_flags_yanked_and_optional_deps():
    d = doc("Pkg_A", requires=["b>=1", 'c; extra == "x"'], releases={"1.0": [file(yanked=True), file(ptype="sdist")], "2.0": []})
    rows = parse_snapshot(d, "2026-01-01T00:00:00+00:00")
    rel = {r["version"]: r for r in rows["releases"]}
    assert rel["1.0"]["yanked"] and rel["1.0"]["has_wheel"] and rel["1.0"]["has_sdist"] and rel["2.0"]["n_files"] == 0
    assert {(x["dep"], x["is_optional"]) for x in rows["deps"]} == {("b", False), ("c", True)}


# ---- crawl ----------------------------------------------------------------------
CHAIN = {"a": doc("a", ["b"]), "b": doc("b", ["c"]), "c": doc("c", ["d"]), "d": doc("d"),
         "e": doc("e", ["f"]), "f": doc("f", ["e"])}


def test_crawl_respects_depth_and_dedupes_cycles(tmp_path):
    fake = FakePyPI(CHAIN)
    stats = crawl(tmp_path, client_for(fake), ["a", "e"], max_depth=2, now=lambda: T0)
    fetched = [n for n, _ in fake.calls]
    assert sorted(fetched) == ["a", "b", "c", "e", "f"], "d is beyond depth 2; the e<->f cycle is fetched once each"
    assert stats["discovered"] == 5


def test_crawl_respects_package_budget(tmp_path):
    stats = crawl(tmp_path, client_for(FakePyPI(CHAIN)), ["a"], max_packages=2, now=lambda: T0)
    assert stats["discovered"] == 2


def test_crawl_records_not_found_and_continues(tmp_path):
    stats = crawl(tmp_path, client_for(FakePyPI({"a": doc("a", ["ghost"])})), ["a"], now=lambda: T0)
    assert stats["not_found"] == 1 and stats["packages_ok"] == 1


def test_incremental_crawl_uses_freshness_then_conditional_requests(tmp_path):
    fake = FakePyPI({"a": doc("a")})
    crawl(tmp_path, client_for(fake), ["a"], now=lambda: T0)
    crawl(tmp_path, client_for(fake), ["a"], now=lambda: T0 + dt.timedelta(hours=1))
    assert len(fake.calls) == 1, "fresh package: no request at all"
    stats = crawl(tmp_path, client_for(fake), ["a"], now=lambda: T0 + dt.timedelta(days=2))
    assert fake.calls[-1][1] == '"a-1"', "stale package: conditional request with the stored ETag"
    assert stats["not_modified"] == 1 and len(list((tmp_path / "bronze" / "pypi").glob("*.gz"))) == 1


def test_changed_serial_stores_a_new_immutable_snapshot(tmp_path):
    fake = FakePyPI({"a": doc("a", serial=1)})
    crawl(tmp_path, client_for(fake), ["a"], now=lambda: T0)
    fake.docs["a"] = doc("a", serial=2)
    crawl(tmp_path, client_for(fake), ["a"], now=lambda: T0 + dt.timedelta(days=2))
    assert sorted(p.name for p in (tmp_path / "bronze" / "pypi").glob("*.gz")) == ["a__1.json.gz", "a__2.json.gz"]


# ---- end to end -----------------------------------------------------------------
def world():
    return {"a": doc("a", ["b", "c"]), "b": doc("b", ["c"]), "c": doc("c", ["a"]),  # a -> b -> c -> a is a cycle
            "d": doc("d", releases={"1.0": [], "2.0": [file(upload="1990-01-01T00:00:00Z")], "3.0": [file()]})}


def q(root, sql):
    return duckdb.connect().execute(sql.replace("{r}", str(root))).fetchall()


def test_end_to_end_quarantine_cycles_and_marts(tmp_path):
    run(tmp_path, ["a", "d"], client_for(FakePyPI(world())), now=lambda: T0)
    reasons = {r[0] for r in q(tmp_path, "SELECT reject_reason FROM read_parquet('{r}/quarantine/releases.parquet')")}
    assert reasons == {"no_files", "implausible_upload_time"}
    reach = dict(q(tmp_path, "SELECT package, transitive_dependents FROM read_parquet('{r}/gold/dependency_reach.parquet')"))
    assert reach == {"a": 3, "b": 3, "c": 3}, "every node in the cycle is reached by all three, and the recursion terminates"
    assert (tmp_path / "report" / "ecosystem_report.html").exists()


def test_rerun_fetches_nothing_and_changes_nothing(tmp_path):
    fake = FakePyPI(world())
    run(tmp_path, ["a", "d"], client_for(fake), now=lambda: T0)
    snap = q(tmp_path, "SELECT count(*), sum(hash(package, version)) FROM read_parquet('{r}/silver/releases.parquet')")
    calls = len(fake.calls)
    run(tmp_path, ["a", "d"], client_for(fake), now=lambda: T0 + dt.timedelta(hours=1))
    assert len(fake.calls) == calls
    assert q(tmp_path, "SELECT count(*), sum(hash(package, version)) FROM read_parquet('{r}/silver/releases.parquet')") == snap


def test_gate_catches_releases_that_reference_unknown_packages(tmp_path):
    import shutil
    from pypipe import quality
    run(tmp_path, ["a"], client_for(FakePyPI(world())), now=lambda: T0)
    rel, bad = f"{tmp_path}/silver/releases.parquet", f"{tmp_path}/silver/bad.parquet"
    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM read_parquet('{rel}') UNION ALL (SELECT 'ghost', version, upload_time, n_files, has_wheel, "
                f"has_sdist, yanked, is_prerelease FROM read_parquet('{rel}') LIMIT 1)) TO '{bad}' (FORMAT PARQUET)")
    shutil.move(bad, rel)
    failed = [r["check"] for r in quality.run_checks(tmp_path, con) if r["severity"] == "critical" and not r["passed"]]
    assert "releases_reference_packages" in failed


def test_snapshot_history_produces_change_events(tmp_path):
    fake = FakePyPI({"a": doc("a", ["b"], serial=1), "b": doc("b"), "c": doc("c")})
    run(tmp_path, ["a"], client_for(fake), now=lambda: T0)
    fake.docs["a"] = doc("a", ["c"], serial=2, releases={"1.0": [file(yanked=True)], "2.0": [file()]})
    run(tmp_path, ["a"], client_for(fake), now=lambda: T0 + dt.timedelta(days=2))
    events = {(r[0], r[1]) for r in q(tmp_path, "SELECT event, detail FROM read_parquet('{r}/silver/changes.parquet') WHERE package = 'a'")}
    assert events == {("release_added", "2.0"), ("release_yanked", "1.0"), ("dependency_added", "c"), ("dependency_removed", "b")}
    summary = dict(q(tmp_path, "SELECT event, events FROM read_parquet('{r}/gold/change_summary.parquet')"))
    assert summary["release_added"] == 1
