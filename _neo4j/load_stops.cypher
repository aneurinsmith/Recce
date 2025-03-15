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
} IN TRANSACTIONS OF 10000 ROWS