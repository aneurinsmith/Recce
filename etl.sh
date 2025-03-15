#! /bin/bash
CSV_DIR="/var/lib/neo4j/import/recce"
TMP_DIR="/tmp/recce_etl"



#----------------#
# Download Files #
#----------------#

# Make temporary directories
echo -e "[ETL] Making temporary directories..."
echo -e "[ETL] -- $CSV_DIR"
mkdir $CSV_DIR
echo -e "[ETL] -- $TMP_DIR"
mkdir $TMP_DIR

# Download bus_data
echo -e "[ETL] Downloading bus_data_wm.zip..."
#wget -O $TMP_DIR/bus_data_wm.zip 'https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/west_midlands/'
echo -e "[ETL] Unzipping bus_data_wm.zip..."
#unzip $TMP_DIR/bus_data_wm.zip -d $CSV_DIR/bus_data_wm



#---------------#
# Load to Neo4j #
#---------------#

# Get neo4j details
read -p "[ETL] Enter Neo4j Username: " username
read -p "[ETL] Enter Neo4j Password: " -s password

# Ensure neo4j indexes exist
echo -e "[ETL] Ensuring neo4j constraints..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/create_index.cypher" | grep -E 'Added|Created|Set|Deleted'

# # Delete redundent stops
# echo -e "[ETL] Deleting unused stops..."
# cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose "
# LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/stops.txt' AS row
# WITH collect(row.stop_id) AS stopIDs

# # // Find stops that aren't in the dataset...
# # MATCH(s:Stop)
# # WHERE NOT s.stop_id IN stopIDs

# # // ...and delete them
# # DETACH DELETE s
# # " | grep -E 'Added|Created|Set|Deleted'

# # Insert and update stops
# echo -e "[ETL] Inserting and updating stops..."
# cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose "
# LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/stops.txt' AS row
# WITH row

# // Only load stops that are a platform or stop
# WHERE row.location_type IS NULL OR toInteger(row.location_type) = 0

# // Insert 10000 rows at a time to save memory
# CALL(row) {
#     MERGE(s:Stop {stop_id: toString(row.stop_id)})
#     SET s.stop_name = toString(row.stop_name),
#         s.accessible = toBoolean(row.wheelchair_boarding),
#         s.location = point({
#             latitude: toFloat(row.stop_lat), 
#             longitude: toFloat(row.stop_lon)
#         })
# } IN TRANSACTIONS OF 10000 ROWS
# " | grep -E 'Added|Created|Set|Deleted'

# # Insert and update parent stops
# echo -e "[ETL] Inserting and updating parent stops..."
# cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose "
# LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data_wm/stops.txt' AS row
# WITH row

# // Match only stations and their child stops
# WHERE row.location_type IS NOT NULL AND toInteger(row.location_type) < 2

# // Create map of all parent stops and their children
# ORDER BY row.stop_id
# WITH apoc.map.groupByMulti(collect(row), 'parent_station') AS cs, collect(row) as rows
# UNWIND rows AS row

# // Select only parent stops and retrieve the child stops from map
# WITH cs, row
# WHERE toInteger(row.location_type) = 1
# WITH row, cs[row.stop_id] AS cs

# // Generate hash
# UNWIND cs AS s
# ORDER BY s.stop_id
# WITH row, collect(DISTINCT s.stop_id) AS hash, collect(s) AS cs
# WITH row, apoc.util.md5(hash) AS hash, cs

# // Create parent stop and relationship
# MERGE (p:Stop {stop_id: toString(hash)})
# ON CREATE SET 
#     p.stop_name = toString(row.stop_name),
#     p.accessible = toBoolean(row.wheelchair_boarding),
#     p.location = point({
#         latitude: toFloat(row.stop_lat), 
#         longitude: toFloat(row.stop_lon)
#     })
# WITH p, cs
# UNWIND cs AS c
# MATCH (s:Stop {stop_id: c.stop_id})
# MERGE (p)<-[:CHILD_OF]-(s)
# " | grep -E 'Added|Created|Set|Deleted'

# # Insert and update parent stops for stops less then 200m apart (daisy chains)
# echo -e "[ETL] Inserting and updating parent stops for stops less then 200m apart (daisy chains)..."
# cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose "
# // Match all stops that don't have a parent and have a similar name
# MATCH (s1:Stop), (s2:Stop)
# WHERE elementId(s1) <> elementId(s2)
#   AND NOT (s1)-[:CHILD_OF]-(:Stop)
#   AND NOT (s2)-[:CHILD_OF]-(:Stop)
#   AND apoc.text.replace(s1.stop_name, '^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$', '') =
#       apoc.text.replace(s2.stop_name, '^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$', '')

# // Group all stops with similar name
# WITH s1, collect(s2) AS s2s
# WITH s1 AS fs, ([s1] + s2s) AS gs

# // Remove duplicate groups
# UNWIND gs AS s
# ORDER BY s.stop_id
# WITH fs, collect(s) AS gs
# WITH DISTINCT gs
# WITH gs[0] AS fs, gs

# // Get the distance of each stop to each other stop
# UNWIND gs AS s
# WITH fs, s, [o IN gs WHERE o <> s | {o:o, dist:point.distance(s.location, o.location)}] AS os
# UNWIND os AS o
# WITH s, o

# // Remove stops that are too far away (100m)
# WHERE o.dist < 100
# WITH s, collect(o.o) as gs
# WITH ([s] + gs) AS gs

# // Group all groups that have shared stops (daisy chain thrice)
# UNWIND gs AS s
# WITH s, collect(gs) AS gs
# WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
# UNWIND gs AS s
# WITH s, collect(gs) AS gs
# WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
# UNWIND gs AS s
# WITH s, collect(gs) AS gs
# WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs

# // Generate hash
# UNWIND gs AS s
# ORDER BY s.stop_id
# WITH gs, collect(s.stop_id) AS hash
# WITH DISTINCT gs, apoc.util.md5(hash) AS hash

# // Get the average lat lng, uid, and name of the grouped stops
# WITH gs,
#     REDUCE(latSum = 0, s IN gs | latSum + s.location.y) / size(gs) AS lat,
#     REDUCE(latSum = 0, s IN gs | latSum + s.location.x) / size(gs) AS lng,
#     apoc.text.replace(gs[0].stop_name, '^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$', '') AS stop_name, hash

# // Create parent stop and relationship
# MERGE (p:Stop {stop_id: toString(hash)})
# ON CREATE SET
#     p.stop_name = toString(stop_name),
#     p.location = point({
#         latitude: toFloat(lat), 
#         longitude: toFloat(lng)
#     })
# WITH p, gs
# UNWIND gs AS g
# MATCH (s:Stop {stop_id: g.stop_id})
# MERGE (p)<-[:CHILD_OF]-(s)
# " | grep -E 'Added|Created|Set|Deleted'
