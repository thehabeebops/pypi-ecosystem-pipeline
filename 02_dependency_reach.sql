-- Blast radius: how many packages in the corpus depend on each package, directly or transitively.
-- UNION (not UNION ALL) de-duplicates, so the recursion terminates even when dependencies form cycles.
WITH RECURSIVE edges AS (
    SELECT DISTINCT d.package AS src, d.dep AS dst
    FROM {deps} d
    WHERE NOT d.is_optional AND d.package <> d.dep AND d.dep IN (SELECT package FROM {packages})
),
reach(src, dst) AS (
    SELECT src, dst FROM edges
    UNION
    SELECT r.src, e.dst FROM reach r JOIN edges e ON r.dst = e.src
),
direct AS (SELECT dst, count(*) AS direct_dependents FROM edges GROUP BY dst)
SELECT r.dst AS package, count(DISTINCT r.src) AS transitive_dependents, d.direct_dependents
FROM reach r JOIN direct d USING (dst)
GROUP BY r.dst, d.direct_dependents
ORDER BY transitive_dependents DESC, package
