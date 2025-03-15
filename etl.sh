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
echo

# Ensure neo4j indexes exist
echo -e "[ETL] Ensuring neo4j constraints..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/create_index.cypher" | grep -E 'Added|Created|Set|Deleted'

# Delete redundent stops
echo -e "[ETL] Deleting unused stops..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/delete_stops.cypher" | grep -E 'Added|Created|Set|Deleted'

# Insert and update stops
echo -e "[ETL] Loading and updating stops..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/load_stops.cypher" | grep -E 'Added|Created|Set|Deleted'

# Insert and update parent stops
echo -e "[ETL] Loading and updating parent stops..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/load_parents.cypher" | grep -E 'Added|Created|Set|Deleted'

# Insert and update parent stops for stops less then 200m apart (daisy chains)
echo -e "[ETL] Generating parent stops for stops less then 200m apart (daisy chains 3x)..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/generate_parents.cypher" | grep -E 'Added|Created|Set|Deleted'

# Insert and update parent stops for stops less then 200m apart (daisy chains)
echo -e "[ETL] Assert that each stop has a maximum of 1 parent"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/assert_parents.cypher" | grep -E 'rows'

