
//------------------------//
// Delete unused agencies //
//------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/agency.txt' AS row
WITH collect(row.agency_id) AS agencyIDs

// Find agencies that aren't in the dataset...
MATCH(a:Agency)
WHERE NOT a.agency_id IN agencyIDs

// ...and delete them
DETACH DELETE a;



//---------------------------//
// Create or update agencies //
//---------------------------//

LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/agency.txt' AS row
WITH row

// Create or update loaded agencies
MERGE(a:Agency {agency_id: toString(row.agency_id)})
SET a.agency_name = toString(row.agency_name);

