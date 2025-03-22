#! /bin/bash
CUR_DIR="$(dirname "$0")"
CSV_DIR="/var/lib/neo4j/import/recce"
TMP_DIR="/tmp/recce_etl"
load_data='y'
mkdir -p "$TMP_DIR"
err_file="$TMP_DIR/log.txt" > "$err_file"


#----------------#
# Download Files #
#----------------#

# Detect if data is already downloaded
if [ -d "$CSV_DIR/bus_data" ]; then
    while true; do
        read -p "[ETL] Bus data already previously downloaded. Would you like to redownload it? (y/n) " load_data
        case $load_data in
            [Yy]* )
                echo -e "[ETL] Proceeding with redownloaded data..."
                break
                ;;
            [Nn]* )
                echo -e "[ETL] Proceeding without redownloading data..."
                break
                ;;
            * )
                echo -e "\033[33m[ETL] Invalid input, please enter y or n\033[0m"
                ;;
        esac
    done
fi

# If not downloaded - or instructed to redownload - download files
if [ $load_data == 'y' ]; then
    echo -e "[ETL] Making temporary directories..."
    
    # Create csv directory
    echo -e "\033[90m[ETL] -- $CSV_DIR/bus_data\033[0m"
    if [ -d "$CSV_DIR/bus_data" ]; then
        rm -rf "$CSV_DIR/bus_data" &>> "$err_file"
    fi
    mkdir "$CSV_DIR/bus_data" &>> "$err_file"

    # Create tmp directory
    echo -e "\033[90m[ETL] -- $TMP_DIR/bus_data\033[0m"
    if [ ! -d "$TMP_DIR/bus_data" ]; then
        mkdir "$TMP_DIR/bus_data" &>> "$err_file"
    fi

    # Fatal exception if any errors encounted when creating directories
    if [ $(wc -l < "$err_file") -gt 0 ]; then
        echo -e "\033[31m[ETL] Fatal error. Check the error log for more details\033[0m"
        exit 1
    fi
    
    # Download and unzip files
    echo -e "[ETL] Downloading bus_data.zip..."
    wget -O "$TMP_DIR/bus_data/bus_data.zip" 'https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/west_midlands/'
    
    echo -e "[ETL] Unzipping bus_data.zip..."
    unzip "$TMP_DIR/bus_data/bus_data.zip" -d "$CSV_DIR/bus_data"
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

echo -e "\033[0m[ETL] Ensuring neo4j constraints and indexes...\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/create_index.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'


# # Load trips and related nodes
echo -e "\033[0m[ETL] Loading and updating service dates...\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_calendars.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'

echo -e "\033[0m[ETL] Loading and updating agencies...\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_agencies.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'

echo -e "\033[0m[ETL] Loading and updating routes...\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_routes.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'

echo -e "\033[0m[ETL] Loading and updating trips...\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_trips.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'


# Load stops and parent stops
echo -e "\033[0m[ETL] Loading and updating stops...\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_stops.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'

echo -e "\033[0m[ETL] Assert that each stop has a maximum of 1 parent"
output=$(cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/assert_parents.cypher" | grep -E 'row' | awk '{print $1}') 

if [[ $output -gt 0 ]]; then
    echo -e "\033[31m[ETL] -- Some stops loaded unsuccessfully!\033[0m"
    exit 1
else
    echo -e "\033[90m[ETL] -- All stops validated successfully.\033[0m"
fi


# Load stop times
echo -e "\033[0m[ETL] Loading and updating stoptimes\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_stoptimes.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'
