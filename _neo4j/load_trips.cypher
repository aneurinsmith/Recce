
//---------------------//
// Delete unused trips //
//---------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/trips.txt' AS row
WITH collect(row.trip_id) AS tripIDs

// Find agencies that aren't in the dataset...
MATCH(t:Trip)
WHERE NOT t.trip_id IN tripIDs

// ...and delete them
DETACH DELETE t;



//------------------------//
// Create or update trips //
//------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/trips.txt' AS row
WITH row

// Create or update loaded agencies
MERGE(t:Trip {trip_id: toString(row.trip_id)})
SET t.headsign = toString(row.trip_headsign)
SET t.accessible = toString(row.wheelchair_accessible)

WITH row, t

MATCH(r:Route {route_id: row.route_id})
MERGE (t)-[:BELONGS_TO]->(r);


