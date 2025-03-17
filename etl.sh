#! /bin/bash
CSV_DIR="/var/lib/neo4j/import/recce"
TMP_DIR="/tmp/recce_etl"



#----------------#
# Download Files #
#----------------#

# Make temporary directories
echo -e "[ETL] Making temporary directories..."
echo -e "[ETL] -- $CSV_DIR"
mkdir $CSV_DIR 2>&1 | awk -e '{printf "\033[0;33m[ETL] -- [ETL] %s\033[0m\n", $0}'
echo -e "[ETL] -- $TMP_DIR"
mkdir $TMP_DIR 2>&1 | awk -e '{printf "\033[0;33m[ETL] -- [ETL] %s\033[0m\n", $0}'

# Download bus_data
echo -e "[ETL] Downloading bus_data_wm.zip..."
# wget -O $TMP_DIR/bus_data_wm.zip 'https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/west_midlands/'
echo -e "[ETL] Unzipping bus_data_wm.zip..."
# unzip $TMP_DIR/bus_data_wm.zip -d $CSV_DIR/bus_data_wm



#---------------#
# Load to Neo4j #
#---------------#

# Get neo4j details
while true; do
    read -p "[ETL] Enter Neo4j Username: " username
    read -p "[ETL] Enter Neo4j Password: " -s password
    echo

    if echo ";" | cypher-shell -a bolt+s://neo4j.aneur.info -u "$username" -p "$password" > /dev/null 2>&1; then
        break
    else 
        echo -e "\033[0;33m[ETL] Authentication failed. Please try again.\033[0m"
    fi
done

echo -e "[ETL] Ensuring neo4j constraints..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/create_index.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'



echo -e "[ETL] Loading and updating agencies"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/load_agencies.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'

echo -e "[ETL] Loading and updating routes"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/load_routes.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'

echo -e "[ETL] Loading and updating trips"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/load_trips.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'



echo -e "[ETL] Loading and updating stops..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/load_stops.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'

echo -e "[ETL] Assert that each stop has a maximum of 1 parent"
output=$(cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "_neo4j/assert_parents.cypher" | 
    grep -E 'row' | awk '{print $1}') 

if [[ $output -gt 0 ]]; then
    echo -e "\033[0;31m[ETL] -- Some stops loaded unsuccessfully!\033[0m"
else
    echo -e "[ETL] -- All stops validated successfully"
fi
