"""Gold marts: SQL files run in numeric order; later marts can reference earlier ones."""
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"


def build_marts(root: Path, con, as_of: str) -> dict:
    s, m = root / "silver", root / "gold"
    m.mkdir(parents=True, exist_ok=True)
    rel = {k: f"read_parquet('{s}/{k}.parquet')" for k in ("packages", "releases", "files", "deps", "classifiers", "changes")}
    rel["as_of"] = as_of
    out = {}
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        name = sql_file.stem.split("_", 1)[1]
        target = m / f"{name}.parquet"
        con.execute(f"COPY ({sql_file.read_text().format(**rel)}) TO '{target}' (FORMAT PARQUET)")
        rel[name] = f"read_parquet('{target}')"
        out[name] = con.execute(f"SELECT count(*) FROM {rel[name]}").fetchone()[0]
    return out
