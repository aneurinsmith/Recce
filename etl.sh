#! /bin/bash
CUR_DIR="$(dirname "$0")"
CSV_DIR="/var/lib/neo4j/import/recce"
TMP_DIR="/tmp"
load_data='y'



#----------------#
# Download Files #
#----------------#

# Detect if they are already installed
if [ -d "$CSV_DIR/bus_data_wm" ]; then
    while true; do
        read -p "[ETL] Bus data already previously loaded. Would you like to reload it? (y/n) " load_data
        case $load_data in
            [Yy]* )
                echo -e "[ETL] Proceeding with reloaded data..."
                break
                ;;
            [Nn]* )
                echo -e "[ETL] Proceeding without reloading data..."
                break
                ;;
            * )
                echo -e "\033[0;33m[ETL] Invalid input, please enter y or n\033[0m"
                ;;
        esac
    done
fi

# If not installed - or instructed to reload them - install them
if [ $load_data == 'y' ]; then
    echo -e "[ETL] Making/clearing temporary directories..."
    
    # Create csv directory
    echo -e "[ETL] -- $CSV_DIR/bus_data_wm"
    if [ -d "$CSV_DIR/bus_data_wm" ]; then
        rm -rf "$CSV_DIR/bus_data_wm" 2>/dev/null
    fi
    mkdir "$CSV_DIR/bus_data_wm" 2>/dev/null

    # Create tmp directory
    echo -e "[ETL] -- $TMP_DIR/recce_etl"
    if [ -d "$TMP_DIR/recce_etl" ]; then
        rm -rf "$TMP_DIR/recce_etl" 2>&1 | awk -e '{printf "\033[0;33m[ETL] -- [ETL] %s\033[0m\n", $0}'
    fi
    mkdir "$TMP_DIR/recce_etl" 2>&1 | awk -e '{printf "\033[0;33m[ETL] -- [ETL] %s\033[0m\n", $0}'

    # Download and unzip files
    echo -e "[ETL] Downloading bus_data_wm.zip..."
    wget -O $TMP_DIR/recce_etl/bus_data_wm.zip 'https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/west_midlands/'
    
    echo -e "[ETL] Unzipping bus_data_wm.zip..."
    unzip $TMP_DIR/recce_etl/bus_data_wm.zip -d $CSV_DIR/bus_data_wm
fi



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

echo -e "[ETL] Ensuring neo4j constraints and indexes..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/create_index.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'



echo -e "[ETL] Loading and updating service dates"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_calendars.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'

echo -e "[ETL] Loading and updating agencies"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_agencies.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'

echo -e "[ETL] Loading and updating routes"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_routes.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'

echo -e "[ETL] Loading and updating trips"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_trips.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'



echo -e "[ETL] Loading and updating stops..."
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_stops.cypher" | 
    grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'

echo -e "[ETL] Assert that each stop has a maximum of 1 parent"
output=$(cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/assert_parents.cypher" | 
    grep -E 'row' | awk '{print $1}') 

if [[ $output -gt 0 ]]; then
    echo -e "\033[0;31m[ETL] -- Some stops loaded unsuccessfully!\033[0m"
else
    echo -e "[ETL] -- All stops validated successfully"
fi



# echo -e "[ETL] Loading and updating stoptimes"
# cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_trips.cypher" | 
#     grep -E 'Added|Created|Set|Deleted' | awk '{print "[ETL] -- " $0}'
