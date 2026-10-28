# Architecture notes

## Layers

| Layer | Contents | Written by | Properties |
|---|---|---|---|
| seeds | `config/seeds.txt`: starting package names | you | The only manual input |
| bronze | `bronze/pypi/<package>__<serial>.json.gz` | `crawl.py` | Immutable, atomic writes, one file per package serial |
| state | `audit/crawl_state.json`: ETag, serial, fetch time, status per package | `crawl.py` | Checkpointed every 25 packages, makes the crawl resumable |
| silver | `packages`, `releases`, `files`, `deps`, `classifiers`, `changes` as Parquet | `silver.py` | Typed and validated, rebuilt from bronze each run |
| quarantine | Rejected records with the rule broken | `silver.py` | Nothing is silently dropped |
| gold | Seven SQL marts as Parquet | `gold.py` | Written only after the quality gate passes |
| report | `report/ecosystem_report.html` | `report.py` | Static, no JavaScript |
| audit | `run_log.jsonl`, `dq_results.jsonl`, `crawl_state.json` | all steps | Full history of every run |

Task order: crawl, silver, quality, marts, report.

## Per-package crawl logic

| Situation | Action |
|---|---|
| Never fetched, or last attempt failed | Fetch, store a snapshot named by the serial |
| Fetched within the freshness window | Skip the request and read the stored snapshot |
| Older than the window | Conditional request with the stored ETag |
| Server answers 304 | Touch the fetch time, reuse the stored snapshot |
| Server answers 200 with a serial already stored | Keep the existing file |
| Server answers 200 with a new serial | Write a new immutable snapshot; the next silver build emits change events |
| 404 | Record `not_found` and continue |
| Retries exhausted | Record `error` and continue; the run reports the error rate |

## HTTP failure handling

| Response | Behaviour |
|---|---|
| 429, 500, 502, 503, 504, network error or timeout | Retry with exponential backoff and jitter; `Retry-After` is honoured |
| 200, 304, 404 | Returned to the crawler |
| Anything else | Fails immediately |

## Silver tables

| Table | Key | Notes |
|---|---|---|
| packages | package | Latest version, license, Python requirement, serial, fetch time |
| releases | package, version | First upload time; wheel and sdist flags; yanked; pre-release flag |
| files | package, version, filename | Type, size, upload time |
| deps | package, dep, marker | Optional flag derived from `extra ==` markers |
| classifiers | package, classifier | Includes the declared Python versions |
| changes | package, to_serial, event, detail | release_added, release_removed, release_yanked, dependency_added, dependency_removed |

## Quality gate

Critical, stop the run: package names unique, releases reference known packages, files reference releases (or a quarantined release), release key unique, no upload time after the snapshot time.

Warnings, recorded: rejected release rate above 5%, crawl error rate above 5%, crawl not-found rate above 5%.

## Marts

Run in numeric order; later marts can read earlier ones: release cadence, dependency reach, Python support, wheel coverage, package health, risk watchlist, change summary.
