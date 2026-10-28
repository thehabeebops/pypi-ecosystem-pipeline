-- A transparent heuristic, NOT a validated model: 40% recency, 30% cadence, 30% supports Python 3.12 or newer.
WITH py AS (
    SELECT package, max(CAST(split_part(regexp_extract(classifier, 'Python :: (3\.[0-9]+)$', 1), '.', 2) AS INTEGER)) AS newest_minor
    FROM {classifiers} WHERE regexp_matches(classifier, 'Python :: 3\.[0-9]+$') GROUP BY 1
)
SELECT c.package, c.releases, c.days_since_last_release, c.releases_last_365d, coalesce(p.newest_minor, 0) AS newest_python_3_minor,
       round(100 * (0.4 * greatest(0, 1 - c.days_since_last_release / 730.0)
                  + 0.3 * least(1, c.releases_last_365d / 12.0)
                  + 0.3 * CAST(coalesce(p.newest_minor, 0) >= 12 AS INTEGER)), 1) AS health_score
FROM {release_cadence} c LEFT JOIN py p USING (package)
ORDER BY health_score DESC, package
