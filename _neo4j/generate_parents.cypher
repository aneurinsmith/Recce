// Match all stops that don't have a parent and have a similar name
MATCH (s1:Stop), (s2:Stop)
WHERE elementId(s1) <> elementId(s2)
  AND NOT (s1)-[:CHILD_OF]-(:Stop)
  AND NOT (s2)-[:CHILD_OF]-(:Stop)
  AND apoc.text.replace(s1.stop_name, '^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$', '') =
      apoc.text.replace(s2.stop_name, '^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$', '')

// Group all stops with similar name
WITH s1, collect(s2) AS s2s
WITH s1 AS fs, ([s1] + s2s) AS gs

// Remove duplicate groups
UNWIND gs AS s
ORDER BY s.stop_id
WITH fs, collect(s) AS gs
WITH DISTINCT gs
WITH gs[0] AS fs, gs

// Get the distance of each stop to each other stop
UNWIND gs AS s
WITH fs, s, [o IN gs WHERE o <> s | {o:o, dist:point.distance(s.location, o.location)}] AS os
UNWIND os AS o
WITH s, o

// Remove stops that are too far away (100m)
WHERE o.dist < 100
WITH s, collect(o.o) as gs
WITH ([s] + gs) AS gs

// Group all groups that have shared stops (daisy chain thrice)
UNWIND gs AS s
WITH s, collect(gs) AS gs
WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
UNWIND gs AS s
WITH s, collect(gs) AS gs
WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
UNWIND gs AS s
WITH s, collect(gs) AS gs
WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs

// Generate hash
UNWIND gs AS s
ORDER BY s.stop_id
WITH gs, collect(s.stop_id) AS hash
WITH DISTINCT gs, apoc.util.md5(hash) AS hash

// Get the average lat lng, uid, and name of the grouped stops
WITH gs,
    REDUCE(latSum = 0, s IN gs | latSum + s.location.y) / size(gs) AS lat,
    REDUCE(latSum = 0, s IN gs | latSum + s.location.x) / size(gs) AS lng,
    apoc.text.replace(gs[0].stop_name, '^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$', '') AS stop_name, hash

// Create parent stop and relationship
MERGE (p:Stop {stop_id: toString(hash)})
ON CREATE SET
    p.stop_name = toString(stop_name),
    p.location = point({
        latitude: toFloat(lat), 
        longitude: toFloat(lng)
    })
WITH p, gs
UNWIND gs AS g
MATCH (s:Stop {stop_id: g.stop_id})
MERGE (p)<-[:CHILD_OF]-(s)