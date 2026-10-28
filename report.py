"""Static HTML report generated from the gold marts (no JavaScript, no external assets)."""
import html
from pathlib import Path

CSS = ("body{font:15px/1.6 system-ui,sans-serif;max-width:920px;margin:40px auto;padding:0 20px;color:#0b0f14}"
       "h1{font-weight:300;font-size:34px}h2{margin-top:44px;font-weight:600;font-size:18px;color:#0b2a4a}"
       "table{border-collapse:collapse;width:100%}td,th{padding:8px 10px;border-bottom:1px solid #e3e6ea;text-align:left;font-size:14px}"
       "th{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:#a86a00}.bar{background:#d9a027;height:10px;border-radius:3px}"
       ".muted{color:#5f6b78;font-size:13px}.kpi{display:flex;gap:40px;flex-wrap:wrap}.kpi b{display:block;font-size:30px;font-weight:300;color:#0b2a4a}")


def _table(rows, headers) -> str:
    th = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><tr>{th}</tr>{body}</table>"


def build(root: Path, con, as_of: str) -> dict:
    g, s = root / "gold", root / "silver"
    q = lambda sql: con.execute(sql).fetchall()
    n_pkg, n_rel, n_files, n_dep = (q(f"SELECT count(*) FROM read_parquet('{s}/{t}.parquet')")[0][0] for t in ("packages", "releases", "files", "deps"))
    top = q(f"SELECT package, transitive_dependents, direct_dependents FROM read_parquet('{g}/dependency_reach.parquet') LIMIT 12")
    py = q(f"SELECT python_version, packages_declaring_support, pct_of_packages FROM read_parquet('{g}/python_support.parquet')")
    risk = q(f"SELECT package, transitive_dependents, days_since_last_release FROM read_parquet('{g}/risk_watchlist.parquet') LIMIT 12")
    fast = q(f"SELECT package, releases, round(median_gap_days, 1) FROM read_parquet('{g}/release_cadence.parquet') WHERE releases >= 30 ORDER BY median_gap_days LIMIT 10")
    chg = q(f"SELECT event, events, packages FROM read_parquet('{g}/change_summary.parquet')")
    bars = "".join(f"<tr><td>{html.escape(v)}</td><td style='width:60%'><div class='bar' style='width:{p}%'></div></td><td>{p}%</td></tr>" for v, _, p in py)
    page = f"""<!doctype html><meta charset=utf-8><title>PyPI ecosystem report</title><style>{CSS}</style>
<h1>PyPI ecosystem report</h1><p class=muted>Snapshot as of {html.escape(as_of)} UTC. Real data from the public PyPI JSON API.</p>
<div class=kpi><div><b>{n_pkg:,}</b>packages</div><div><b>{n_rel:,}</b>releases</div><div><b>{n_files:,}</b>files</div><div><b>{n_dep:,}</b>dependency edges</div></div>
<h2>Most depended-upon packages (transitive dependents in this corpus)</h2>{_table(top, ["package", "transitive dependents", "direct dependents"])}
<h2>Python 3 support declared by the latest release</h2><table>{bars}</table>
<h2>Risk watchlist: widely depended-upon, no release in over a year</h2>{_table(risk, ["package", "transitive dependents", "days since last release"]) if risk else "<p class=muted>None in this corpus.</p>"}
<h2>Fastest release cadence (30+ stable releases)</h2>{_table(fast, ["package", "releases", "median days between releases"])}
<h2>Changes since the previous snapshot</h2>{_table(chg, ["event", "events", "packages"]) if chg else "<p class=muted>No earlier snapshot yet. Run the pipeline again later to see releases, yanks and dependency changes.</p>"}
<p class=muted>Health scores are a transparent heuristic (40% recency, 30% cadence, 30% Python 3.12+ support), not a validated model.</p>"""
    out = root / "report"
    out.mkdir(parents=True, exist_ok=True)
    (out / "ecosystem_report.html").write_text(page)
    return {"report": str(out / "ecosystem_report.html")}
