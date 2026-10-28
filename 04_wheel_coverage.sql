-- Share of stable releases that ship at least one wheel (faster, safer installs than building from source).
SELECT package, count(*) AS stable_releases,
       round(100.0 * count(*) FILTER (WHERE has_wheel) / count(*), 1) AS pct_with_wheel,
       count(*) FILTER (WHERE yanked) AS yanked_releases
FROM {releases} WHERE NOT is_prerelease
GROUP BY package ORDER BY package
