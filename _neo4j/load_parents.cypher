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
MERGE (p)<-[:CHILD_OF]-(s)