// CALL apoc.trigger.add('assert-single-parent',
// 'UNWIND $createdRelationships AS rel
//  WITH rel
//  WHERE type(rel) = "CHILD_OF"
//  MATCH (c:Stop)-[:CHILD_OF]->(p:Stop)
//  WITH rel, c, count(p) AS pc, collect(p) AS ps
//  UNWIND ps AS p
//  WITH c, pc, p, endNode(rel) AS np
//  WHERE pc > 1 AND p <> np
//  CALL apoc.util.validate(true, "A Stop node can only have one parent: " + c.stop_id, [])
//  RETURN c',
// {phase: 'before', batchSize: 1000},{});

CREATE CONSTRAINT IF NOT EXISTS FOR (a:Agency) REQUIRE a.agency_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (r:Route) REQUIRE r.route_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (t:Trip) REQUIRE t.trip_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (c:Calendar) REQUIRE c.service_id IS UNIQUE;
CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stop) REQUIRE s.stop_id IS UNIQUE;

CREATE POINT INDEX IF NOT EXISTS FOR (v:Venue) ON v.location;
CREATE POINT INDEX IF NOT EXISTS FOR (s:Stop) ON s.location;
CREATE TEXT INDEX IF NOT EXISTS FOR (s:Stop) ON s.stop_name;
CREATE TEXT INDEX IF NOT EXISTS FOR (r:Route) ON r.route_name;
CREATE RANGE INDEX IF NOT EXISTS FOR (st:StopTime) ON st.seq;
CREATE RANGE INDEX IF NOT EXISTS FOR (st:StopTime) ON st.arrival_time;
CREATE RANGE INDEX IF NOT EXISTS FOR (st:StopTime) ON st.departure_time;

CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.mon;
CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.tue;
CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.wed;
CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.thu;
CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.fri;
CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.sat;
CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.sun;
