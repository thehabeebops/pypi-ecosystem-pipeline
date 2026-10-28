-- Release rhythm per package, ignoring pre-releases.
WITH r AS (SELECT package, upload_time AS ts FROM {releases} WHERE NOT is_prerelease),
g AS (SELECT package, ts, date_diff('day', lag(ts) OVER (PARTITION BY package ORDER BY ts), ts) AS gap FROM r)
SELECT package, count(*) AS releases, min(ts) AS first_release, max(ts) AS last_release,
       median(gap) AS median_gap_days,
       count(*) FILTER (WHERE ts >= TIMESTAMP '{as_of}' - INTERVAL 365 DAY) AS releases_last_365d,
       date_diff('day', max(ts), TIMESTAMP '{as_of}') AS days_since_last_release
FROM g GROUP BY package ORDER BY package
