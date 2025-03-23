
//------------------------//
// Delete unused agencies //
//------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/agency.txt' AS row
WITH collect(row.agency_id) AS agencyIDs

// Find agencies that aren't in the dataset...
MATCH(a:Agency)
WHERE NOT a.agency_id IN agencyIDs

// ...and delete them
DETACH DELETE a;


// Delete agencies that are wrongly connected
LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/routes.txt' AS row
WITH row

MATCH (a:Agency)-[:OPERATES]->(:Route {route_id: row.route_id})
WHERE NOT a.agency_id = row.agency_id

DETACH DELETE a;


//---------------------------//
// Create or update agencies //
//---------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/agency.txt' AS row
WITH row

// Create or update loaded agencies
MERGE(a:Agency {agency_id: toString(row.agency_id)})
SET a.agency_name = toString(row.agency_name);

