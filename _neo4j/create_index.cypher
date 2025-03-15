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

CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stop) REQUIRE s.stop_id IS UNIQUE;
CREATE INDEX IF NOT EXISTS FOR (s:Stop) ON s.location;