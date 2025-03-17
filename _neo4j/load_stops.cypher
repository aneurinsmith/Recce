
//--------------------------------//
// Delete unused stops and parent //
//--------------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/stops.txt' AS row
WITH collect(row.stop_id) AS stopIDs

// Find stops that aren't in the dataset...
MATCH(s:Stop)
WHERE NOT s.stop_id IN stopIDs

// ...and delete them
DETACH DELETE s;



//------------------------//
// Create or update stops //
//------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/stops.txt' AS row
WITH row

// Only load stops that are a platform or stop
WHERE row.location_type IS NULL OR toInteger(row.location_type) = 0

// Insert 10000 rows at a time to save memory
CALL(row) {
    MERGE(s:Stop {stop_id: toString(row.stop_id)})
    SET s.stop_name = toString(row.stop_name),
        s.accessible = toBoolean(row.wheelchair_boarding),
        s.location = point({
            latitude: toFloat(row.stop_lat), 
            longitude: toFloat(row.stop_lon)
        })
} IN TRANSACTIONS OF 10000 ROWS;



//---------------------//
// Create parent stops //
//---------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/stops.txt' AS row
WITH row

// Match only stations and their child stops
WHERE row.location_type IS NOT NULL AND toInteger(row.location_type) < 2

// Create map of all parent stops and their children
ORDER BY row.stop_id
WITH apoc.map.groupByMulti(collect(row), 'parent_station') AS cs, collect(row) as rows
UNWIND rows AS row

// Select only parent stops and retrieve the child stops from map
WITH cs, row
WHERE toInteger(row.location_type) = 1
WITH row, cs[row.stop_id] AS cs

// Generate hash
UNWIND cs AS s
ORDER BY s.stop_id
WITH row, collect(DISTINCT s.stop_id) AS hash, collect(s) AS cs
WITH row, apoc.util.md5(hash) AS hash, cs

// Create parent stop and relationship
MERGE (p:Stop {stop_id: toString(hash)})
ON CREATE SET 
    p.stop_name = toString(row.stop_name),
    p.accessible = toBoolean(row.wheelchair_boarding),
    p.location = point({
        latitude: toFloat(row.stop_lat), 
        longitude: toFloat(row.stop_lon)
    })
WITH p, cs
UNWIND cs AS c
MATCH (s:Stop {stop_id: c.stop_id})
MERGE (p)<-[:CHILD_OF]-(s);



//-----------------------//
// Generate parent stops //
//-----------------------//

// Match all stops that don't have a parent and have a similar name
MATCH (s1:Stop)
WHERE NOT (s1)-[:CHILD_OF]-(:Stop)
WITH s1
MATCH (s2:Stop)
WHERE NOT (s2)-[:CHILD_OF]-(:Stop)
  AND elementId(s1) <> elementId(s2)
//   AND substring(s1.stop_id, 0, size(s1.stop_id) -3) =
//       substring(s2.stop_id, 0, size(s2.stop_id) -3)
  AND substring(s1.stop_id, 0, 4) =
      substring(s2.stop_id, 0, 4)
  AND point.distance(s1.location, s2.location) < 60
  AND apoc.text.jaroWinklerDistance(
        apoc.text.replace(s1.stop_name,'^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$',''), 
        apoc.text.replace(s2.stop_name,'^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$','')
      ) < .2

// Group all stops with similar name
WITH s1, collect(s2) AS s2s
WITH s1 AS fs, ([s1] + s2s) AS gs

// Remove duplicate groups
UNWIND gs AS s
ORDER BY s.stop_id
WITH fs, collect(s) AS gs
WITH DISTINCT gs
WITH gs[0] AS fs, gs

// Group all groups that have shared stops (daisy 6x)
UNWIND gs AS s
WITH s, collect(gs) AS gs
WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
UNWIND gs AS s
WITH s, collect(gs) AS gs
WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
UNWIND gs AS s
WITH s, collect(gs) AS gs
WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
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
WITH gs, collect(s) AS gso, collect(s.stop_id) AS hash
WITH DISTINCT gso AS gs, apoc.util.md5(hash) AS hash

// Get the average lat lng, uid, and name of the grouped stops
WITH gs,
  REDUCE(latSum = 0, s IN gs | latSum + s.location.y) / size(gs) AS lat,
  REDUCE(latSum = 0, s IN gs | latSum + s.location.x) / size(gs) AS lng,
  apoc.text.replace(gs[0].stop_name,'^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$','') AS stop_name, hash

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
MERGE (p)<-[:CHILD_OF]-(s);

