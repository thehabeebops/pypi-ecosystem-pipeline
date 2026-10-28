"""Pure functions: one raw PyPI JSON document in, normalised rows out."""
import re

from .crawl import normalize, parse_requirement

_PRE = re.compile(r"(?i)(?:\d|\.)(?:a|b|rc|c|alpha|beta|pre|preview|dev)\.?\d*(?:$|\.)")


def is_prerelease(version: str) -> bool:
    return bool(_PRE.search(version))


def _iso(value):
    return value.rstrip("Z") if isinstance(value, str) else None


def parse_snapshot(doc: dict, fetched_at: str) -> dict:
    info = doc["info"]
    name = normalize(info["name"])
    classifiers = info.get("classifiers") or []
    license_ = info.get("license_expression") or next(
        (c.split("::")[-1].strip() for c in classifiers if c.startswith("License ::")), None)
    out = {
        "packages": [{"package": name, "display_name": info["name"], "summary": info.get("summary"),
                      "latest_version": info.get("version"), "requires_python": info.get("requires_python"),
                      "license": license_, "last_serial": doc.get("last_serial"),
                      "yanked_latest": bool(info.get("yanked")), "fetched_at": fetched_at}],
        "releases": [], "files": [], "deps": [], "classifiers": [],
    }
    for version, files in (doc.get("releases") or {}).items():
        times = [t for t in (_iso(f.get("upload_time_iso_8601") or f.get("upload_time")) for f in files) if t]
        out["releases"].append({
            "package": name, "version": version, "upload_time": min(times) if times else None, "n_files": len(files),
            "has_wheel": any(f.get("packagetype") == "bdist_wheel" for f in files),
            "has_sdist": any(f.get("packagetype") == "sdist" for f in files),
            "yanked": any(bool(f.get("yanked")) for f in files), "is_prerelease": is_prerelease(version)})
        for f in files:
            out["files"].append({"package": name, "version": version, "filename": f.get("filename"),
                                 "packagetype": f.get("packagetype"), "python_version": f.get("python_version"),
                                 "size": f.get("size"), "upload_time": _iso(f.get("upload_time_iso_8601") or f.get("upload_time")),
                                 "yanked": bool(f.get("yanked"))})
    for req in info.get("requires_dist") or []:
        dep, spec, marker, optional = parse_requirement(req)
        if dep:
            out["deps"].append({"package": name, "dep": dep, "specifier": spec, "marker": marker, "is_optional": optional})
    out["classifiers"] = [{"package": name, "classifier": c} for c in classifiers]
    return out


def diff_snapshots(prev: dict, new: dict, from_serial: int, to_serial: int, detected_at: str) -> list[dict]:
    """Change events between two consecutive snapshots of the same package (output of parse_snapshot)."""
    name = new["packages"][0]["package"]
    base = {"package": name, "from_serial": from_serial, "to_serial": to_serial, "detected_at": detected_at}
    pv, nv = ({r["version"]: r for r in s["releases"]} for s in (prev, new))
    events = [dict(base, event="release_added", detail=v) for v in sorted(nv.keys() - pv.keys())]
    events += [dict(base, event="release_removed", detail=v) for v in sorted(pv.keys() - nv.keys())]
    events += [dict(base, event="release_yanked", detail=v) for v in sorted(nv.keys() & pv.keys()) if nv[v]["yanked"] and not pv[v]["yanked"]]
    pd, nd = ({d["dep"] for d in s["deps"] if not d["is_optional"]} for s in (prev, new))
    events += [dict(base, event="dependency_added", detail=d) for d in sorted(nd - pd)]
    events += [dict(base, event="dependency_removed", detail=d) for d in sorted(pd - nd)]
    return events
