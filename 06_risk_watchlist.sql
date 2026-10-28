-- Widely depended-upon packages that have not shipped a release in over a year.
SELECT h.package, r.transitive_dependents, h.days_since_last_release, h.health_score
FROM {package_health} h JOIN {dependency_reach} r USING (package)
WHERE r.transitive_dependents >= 5 AND h.days_since_last_release > 365
ORDER BY r.transitive_dependents DESC, h.package
