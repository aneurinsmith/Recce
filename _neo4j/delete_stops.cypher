LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/stops.txt' AS row
WITH collect(row.stop_id) AS stopIDs

// Find stops that aren't in the dataset...
MATCH(s:Stop)
WHERE NOT s.stop_id IN stopIDs

// ...and delete them
DETACH DELETE s