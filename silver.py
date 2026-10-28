"""Silver: typed, validated tables built from the latest raw snapshot of every crawled package.

Records that break a rule go to quarantine with the reason. Nothing is silently dropped.
"""
import json
from pathlib import Path

from .crawl import load_snapshot
from .parse import diff_snapshots, parse_snapshot

SCHEMAS = {
    "packages": "{'package':'VARCHAR','display_name':'VARCHAR','summary':'VARCHAR','latest_version':'VARCHAR','requires_python':'VARCHAR','license':'VARCHAR','last_serial':'BIGINT','yanked_latest':'BOOLEAN','fetched_at':'VARCHAR'}",
    "releases": "{'package':'VARCHAR','version':'VARCHAR','upload_time':'VARCHAR','n_files':'INTEGER','has_wheel':'BOOLEAN','has_sdist':'BOOLEAN','yanked':'BOOLEAN','is_prerelease':'BOOLEAN'}",
    "files": "{'package':'VARCHAR','version':'VARCHAR','filename':'VARCHAR','packagetype':'VARCHAR','python_version':'VARCHAR','size':'BIGINT','upload_time':'VARCHAR','yanked':'BOOLEAN'}",
    "deps": "{'package':'VARCHAR','dep':'VARCHAR','specifier':'VARCHAR','marker':'VARCHAR','is_optional':'BOOLEAN'}",
    "classifiers": "{'package':'VARCHAR','classifier':'VARCHAR'}",
    "changes": "{'package':'VARCHAR','event':'VARCHAR','detail':'VARCHAR','from_serial':'BIGINT','to_serial':'BIGINT','detected_at':'VARCHAR'}",
}


def _write(con, sql: str, path: Path) -> int:
    con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
    return con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]


def build(root: Path, con) -> dict:
    state = json.loads((root / "audit" / "crawl_state.json").read_text())
    s, q, tmp = root / "silver", root / "quarantine", root / "silver" / "_tmp"
    for d in (s, q, tmp):
        d.mkdir(parents=True, exist_ok=True)
    handles = {k: open(tmp / f"{k}.jsonl", "w") for k in SCHEMAS}
    for name, st in sorted(state.items()):
        if st.get("status") != "ok":
            continue
        rows = parse_snapshot(load_snapshot(root / st["path"]), st["fetched_at"])
        for table, recs in rows.items():
            for rec in recs:
                handles[table].write(json.dumps(rec) + "\n")
        # Every pair of consecutive immutable snapshots yields change events, so history survives rebuilds.
        snaps = sorted((root / "bronze" / "pypi").glob(f"{name}__*.json.gz"), key=lambda p: int(p.name.split("__")[1].split(".")[0]))
        for older, newer in zip(snaps, snaps[1:]):
            serials = [int(p.name.split("__")[1].split(".")[0]) for p in (older, newer)]
            for ev in diff_snapshots(parse_snapshot(load_snapshot(older), ""), parse_snapshot(load_snapshot(newer), ""),
                                     serials[0], serials[1], st["fetched_at"]):
                handles["changes"].write(json.dumps(ev) + "\n")
    for h in handles.values():
        h.close()
    for table, cols in SCHEMAS.items():
        con.execute(f"CREATE OR REPLACE TEMP TABLE raw_{table} AS SELECT * FROM read_json('{tmp / (table + '.jsonl')}', "
                    f"format='newline_delimited', columns={cols})")
    stats = {}
    stats["packages"] = _write(con, "SELECT * REPLACE (CAST(replace(fetched_at, '+00:00', '') AS TIMESTAMP) AS fetched_at) "
                                    "FROM raw_packages WHERE package <> '' ORDER BY package", s / "packages.parquet")
    con.execute("""CREATE OR REPLACE TEMP TABLE r_checked AS
        SELECT r.*, try_cast(upload_time AS TIMESTAMP) AS ts,
               CASE WHEN n_files = 0 THEN 'no_files'
                    WHEN try_cast(upload_time AS TIMESTAMP) IS NULL THEN 'bad_upload_time'
                    WHEN try_cast(upload_time AS TIMESTAMP) < TIMESTAMP '2003-01-01' THEN 'implausible_upload_time' END AS reject_reason
        FROM raw_releases r""")
    stats["releases_in"] = con.execute("SELECT count(*) FROM r_checked").fetchone()[0]
    stats["releases"] = _write(con, "SELECT package, version, ts AS upload_time, n_files, has_wheel, has_sdist, yanked, is_prerelease "
                                    "FROM r_checked WHERE reject_reason IS NULL ORDER BY package, ts, version", s / "releases.parquet")
    stats["releases_rejected"] = _write(con, "SELECT 'releases' AS entity, package || '==' || version AS record_key, reject_reason "
                                             "FROM r_checked WHERE reject_reason IS NOT NULL", q / "releases.parquet")
    stats["files"] = _write(con, "SELECT package, version, filename, packagetype, python_version, size, try_cast(upload_time AS TIMESTAMP) AS upload_time, yanked "
                                 "FROM raw_files WHERE filename IS NOT NULL AND coalesce(size, 0) >= 0 ORDER BY package, version, filename", s / "files.parquet")
    stats["deps"] = _write(con, "SELECT * FROM raw_deps ORDER BY package, dep, marker", s / "deps.parquet")
    stats["classifiers"] = _write(con, "SELECT * FROM raw_classifiers ORDER BY package, classifier", s / "classifiers.parquet")
    stats["changes"] = _write(con, "SELECT * FROM raw_changes ORDER BY package, to_serial, event, detail", s / "changes.parquet")
    stats["as_of"] = str(con.execute(f"SELECT max(fetched_at) FROM read_parquet('{s}/packages.parquet')").fetchone()[0])
    return stats
