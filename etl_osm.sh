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
if [ -d "$CSV_DIR/osm_data" ]; then
    while true; do
        read -p "[ETL] OSM data already previously downloaded. Would you like to redownload it? (y/n) " load_data
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
    echo -e "\033[90m[ETL] -- $CSV_DIR/osm_data\033[0m"
    if [ -d "$CSV_DIR/osm_data" ]; then
        rm -rf "$CSV_DIR/osm_data" &>> "$err_file"
    fi
    mkdir "$CSV_DIR/osm_data" &>> "$err_file"

    # Create tmp directory
    echo -e "\033[90m[ETL] -- $TMP_DIR/recce_etl\033[0m"
    if [ ! -d "$TMP_DIR/osm_data" ]; then
        mkdir "$TMP_DIR/osm_data" &>> "$err_file"
    fi

    # Fatal exception if any errors encounted when creating directories
    if [ $(wc -l < "$err_file") -gt 0 ]; then
        echo -e "\033[31m[ETL] Fatal error. Check the error log for more details\033[0m"
        exit 1
    fi
    
    # Download the osm data
    echo -e "[ETL] Downloading osm_data.osm.pbf..."
    wget -O "$TMP_DIR/osm_data/osm_data.osm.pbf" "https://download.geofabrik.de/europe/united-kingdom/england/staffordshire-latest.osm.pbf"

fi


#--------------------#
# Generate CSV files #
#--------------------#

echo -e "[ETL] Generating CSV files..."
echo -e "\033[90m[ETL] -- Converting to usable format\033[0m"
osmconvert "$TMP_DIR/osm_data/osm_data.osm.pbf" --all-to-nodes -o="$TMP_DIR/osm_data/output.o5m" &>> "$err_file"

echo -e "\033[90m[ETL] -- Creating filtered o5m files for desired venue categories\033[0m"
osmfilter "$TMP_DIR/osm_data/output.o5m" -o="$TMP_DIR/osm_data/amenity.o5m" --keep="amenity=pub =cinema =theatre =cafe =bicycle_rental =music_venue =boat_rental =bar" --ignore-dependencies &>> "$err_file"
osmfilter "$TMP_DIR/osm_data/output.o5m" -o="$TMP_DIR/osm_data/tourism.o5m" --keep="tourism=attraction =museum =gallery =theme_park =zoo =aquarium" --ignore-dependencies &>> "$err_file"
osmfilter "$TMP_DIR/osm_data/output.o5m" -o="$TMP_DIR/osm_data/natural.o5m" --keep="natural=beach =park =waterfall" --ignore-dependencies &>> "$err_file"
osmfilter "$TMP_DIR/osm_data/output.o5m" -o="$TMP_DIR/osm_data/leisure.o5m" --keep="leisure=park =garden =nature_reserve" --ignore-dependencies &>> "$err_file"
osmfilter "$TMP_DIR/osm_data/output.o5m" -o="$TMP_DIR/osm_data/historic.o5m" --keep="historic=castle =fort =monument =farm =archaeological_site =battlefield =ruins =city_gate =building =house =aircraft =aqueduct" --ignore-dependencies &>> "$err_file"

# # Convert to csv
echo -e "\033[90m[ETL] -- Converting to CSV\033[0m"
osmconvert "$TMP_DIR/osm_data/amenity.o5m" -o="$CSV_DIR/osm_data/amenity.csv" --csv="@id @lon @lat name amenity wikidata" --csv-headline --csv-separator=',' &>> "$err_file"
osmconvert "$TMP_DIR/osm_data/tourism.o5m" -o="$CSV_DIR/osm_data/tourism.csv" --csv="@id @lon @lat name tourism wikidata" --csv-headline --csv-separator=',' &>> "$err_file"
osmconvert "$TMP_DIR/osm_data/natural.o5m" -o="$CSV_DIR/osm_data/natural.csv" --csv="@id @lon @lat name natural wikidata" --csv-headline --csv-separator=',' &>> "$err_file"
osmconvert "$TMP_DIR/osm_data/leisure.o5m" -o="$CSV_DIR/osm_data/leisure.csv" --csv="@id @lon @lat name leisure wikidata" --csv-headline --csv-separator=',' &>> "$err_file"
osmconvert "$TMP_DIR/osm_data/historic.o5m" -o="$CSV_DIR/osm_data/historic.csv" --csv="@id @lon @lat name historic wikidata" --csv-headline --csv-separator=',' &>> "$err_file"

# Fatal exception if any errors encounted when generating CSV files
if [ $(wc -l < "$err_file") -gt 0 ]; then
    echo -e "\033[31m[ETL] Fatal error. Check the error log for more details\033[0m"
    exit 1
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

echo -e "\033[0m[ETL] Loading venues into database...\033[90m"
cypher-shell -a bolt+s://neo4j.aneur.info -u $username -p $password --format verbose < "$CUR_DIR/_neo4j/load_venues.cypher" >&1 | 
    awk '/Added|Created|Set|Deleted/ {print "[ETL] -- " $0}'



# https://www.wikidata.org/wiki/Special:EntityData/<wikidata id goes here>.json
# https://commons.wikimedia.org/w/index.php?title=Special:Redirect/file/<image name goes here>&width=300

# P8264 = attribution text
# P18 = image details