-- Share of packages whose latest release declares support for each Python 3 minor version.
SELECT regexp_extract(classifier, 'Python :: (3\.[0-9]+)$', 1) AS python_version,
       count(DISTINCT package) AS packages_declaring_support,
       round(100.0 * count(DISTINCT package) / (SELECT count(*) FROM {packages}), 1) AS pct_of_packages
FROM {classifiers}
WHERE regexp_matches(classifier, 'Python :: 3\.[0-9]+$')
GROUP BY 1
ORDER BY CAST(split_part(python_version, '.', 2) AS INTEGER)
