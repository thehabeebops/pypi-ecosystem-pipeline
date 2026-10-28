"""Data quality gates for the silver layer and the crawl."""
from pathlib import Path


def run_checks(root: Path, con, crawl_stats: dict | None = None, as_of: str | None = None) -> list[dict]:
    s, q = root / "silver", root / "quarantine"
    t = {k: f"read_parquet('{s}/{k}.parquet')" for k in ("packages", "releases", "files", "deps")}
    checks = [
        ("package_names_unique", "critical", f"SELECT count(*) - count(DISTINCT package) FROM {t['packages']}", 0),
        ("releases_reference_packages", "critical", f"SELECT count(*) FROM {t['releases']} r ANTI JOIN {t['packages']} p USING (package)", 0),
        ("files_reference_releases", "critical",
         f"SELECT count(*) FROM {t['files']} f ANTI JOIN {t['releases']} r USING (package, version) "
         f"WHERE f.package || '==' || f.version NOT IN (SELECT record_key FROM read_parquet('{q}/releases.parquet'))", 0),
        ("release_key_unique", "critical", f"SELECT count(*) - count(DISTINCT (package, version)) FROM {t['releases']}", 0),
        ("no_future_upload_times", "critical", f"SELECT count(*) FROM {t['releases']} WHERE upload_time > TIMESTAMP '{as_of or '2100-01-01'}'", 0),
        ("rejected_release_rate_pct", "warn",
         f"SELECT round(100.0 * (SELECT count(*) FROM read_parquet('{q}/releases.parquet')) / "
         f"greatest(1, (SELECT count(*) FROM read_parquet('{q}/releases.parquet')) + (SELECT count(*) FROM {t['releases']})), 2)", 5.0),
    ]
    results = []
    for name, severity, sql, threshold in checks:
        value = con.execute(sql).fetchone()[0]
        results.append({"check": name, "severity": severity, "value": float(value), "threshold": threshold, "passed": value <= threshold})
    if crawl_stats:
        disc = max(1, crawl_stats.get("discovered", 0))
        for name, value, limit in (("crawl_error_rate_pct", 100.0 * crawl_stats.get("errors", 0) / disc, 5.0),
                                   ("crawl_not_found_rate_pct", 100.0 * crawl_stats.get("not_found", 0) / disc, 5.0)):
            results.append({"check": name, "severity": "warn", "value": round(value, 2), "threshold": limit, "passed": value <= limit})
    return results
