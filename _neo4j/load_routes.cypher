
//----------------------//
// Delete unused routes //
//----------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/routes.txt' AS row
WITH collect(row.route_id) AS routeIDs

// Find routes that aren't in the dataset...
MATCH(r:Route)
WHERE NOT r.route_id IN routeIDs

// ...and delete them
DETACH DELETE r;



//-------------------------//
// Create or update routes //
//-------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/routes.txt' AS row
WITH row

// Create or update loaded routes
MERGE(r:Route {route_id: toString(row.route_id)})
SET r.route_name = toString(row.route_short_name)

WITH row, r

MATCH(a:Agency {agency_id: row.agency_id})
MERGE (a)-[:OPERATES]->(r);


