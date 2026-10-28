-- What changed between consecutive snapshots of each package, counted by event type.
SELECT event, count(*) AS events, count(DISTINCT package) AS packages
FROM {changes}
GROUP BY event
ORDER BY events DESC, event
