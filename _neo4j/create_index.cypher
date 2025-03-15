CALL apoc.trigger.add('assert-single-parent',
'UNWIND $createdRelationships AS rel
 WITH rel
 WHERE type(rel) = "CHILD_OF"
 MATCH (child:Stop)-[:CHILD_OF]->(parent:Stop)
 WITH child, COUNT(parent) AS parentCount
 WHERE parentCount > 1
 CALL apoc.util.validate(true, "A Stop node can only have one parent.", [])
 RETURN child',
{phase: 'before'},{});

CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stop) REQUIRE s.stop_id IS UNIQUE;
CREATE INDEX IF NOT EXISTS FOR (s:Stop) ON s.location;