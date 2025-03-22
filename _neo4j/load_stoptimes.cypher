
//--------------------------//
// Delete unused stop times //
//--------------------------//

// Delete all stops times that dont have a valid trip_id
LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stop_times.txt' AS row
WITH collect(DISTINCT row.trip_id) AS tripIDs

MATCH (st:StopTime)-[:PART_OF]->(t:Trip)
WHERE NOT t.trip_id IN tripIDs

DETACH DELETE st;


// Delete all stop times that are beyond the max stop sequence
LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stop_times.txt' AS row
WITH row.trip_id AS trip_id, toInteger(last(collect(row.stop_sequence))) AS max_seq

MATCH (st:StopTime)-[:PART_OF]->(t:Trip {trip_id:trip_id})
WHERE st.seq > max_seq

DETACH DELETE st;


//-----------------------------//
// Create or update stop times //
//-----------------------------//

// Create the first 100000 StopTime nodes
LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stop_times.txt' AS row
WITH row LIMIT 100000

MATCH(t:Trip {trip_id: row.trip_id}), (s:Stop {stop_id: row.stop_id})

MERGE (t)<-[:PART_OF]-(st:StopTime {seq: toInteger(row.stop_sequence)})<-[:TIME_TABLED]-(s)
SET st.arrival_time = toString(row.arrival_time),
    st.departure_time = toString(row.departure_time);


// Create the remaining StopTime nodes
LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stop_times.txt' AS row
WITH row SKIP 100000

CALL(row) {
    MATCH(t:Trip {trip_id: row.trip_id}), (s:Stop {stop_id: row.stop_id})

    MERGE (t)<-[:PART_OF]-(st:StopTime {seq: toInteger(row.stop_sequence)})<-[:TIME_TABLED]-(s)
    SET st.arrival_time = toString(row.arrival_time),
        st.departure_time = toString(row.departure_time)
} IN TRANSACTIONS OF 100000 ROWS;


// Link all stop times that precede one another
MATCH (st1:StopTime)-[:PART_OF]->(t:Trip)
MATCH (st2:StopTime)-[:PART_OF]->(t)
WHERE st2.seq=(st1.seq+1)

CALL (st1, st2) {
    MERGE (st1)-[pre:PRECEDES]->(st2)
    SET pre.timeTraveled = 
        (toInteger(split(st2.departure_time, ':')[0])*3600 +
        toInteger(split(st2.departure_time, ':')[1])*60 +
        toInteger(split(st2.departure_time, ':')[2])) -
        (toInteger(split(st1.arrival_time, ':')[0])*3600 +
        toInteger(split(st1.arrival_time, ':')[1])*60 +
        toInteger(split(st1.arrival_time, ':')[2]))
} IN TRANSACTIONS OF 100000 ROWS;
