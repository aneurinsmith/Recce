
import os
import shutil
import wget
import zipfile
import osmium

from shapely.geometry import MultiPolygon, Polygon, Point
from logger import Console, Level
from query import QueryData, Database
from argparse import ArgumentParser

# region set log level
parser = ArgumentParser(description="ETL script for extracting bus and osm data and loading it into a Neo4j database")
parser.add_argument("-l", "--level", "--log-level", help="The minimum level required for a message to be logged", type=str, default="DEBUG")

parse_level = parser.parse_args().level
log_level = Console._min_lvl

try:
    if parse_level.isdigit() and int(parse_level) < 3:
        log_level = Level(int(parse_level))
    else:
        log_level = Level[parse_level.upper()]

except KeyError:
    parser.error(f"Invalid level argument: {parse_level}. Must be (0, 1, 2) or [TRACE, DEBUG, INFO]")

Console.set_level(log_level)

CSV_DIR = "/var/lib/neo4j/import/recce"
TMP_DIR = "/tmp/recce"
# endregion set log level


Console.log(Level.NONE, """

██████  ███████  ██████  ██████ ███████         ███████ ████████ ██      
██   ██ ██      ██      ██      ██              ██         ██    ██      
██████  █████   ██      ██      █████           █████      ██    ██      
██   ██ ██      ██      ██      ██              ██         ██    ██      
██   ██ ███████  ██████  ██████ ███████         ███████    ██    ███████ 

A python script for extracting, transforming, 
and loading OSM venues and GTFS Bus data into 
a neo4j database

Created by: Aneurin F. Smith

""")


try:

# region download data

    def reload_directory(dir: str) -> bool:
        reload = False
        if os.path.isdir(dir):
            Console.log(Level.INFO, f"\033[4m{dir}\033[0m already exists")
            while True:
                inp_reload = Console.inp(f"Would you like to reload the directory?", False, "no").lower()

                if inp_reload in ['y', "yes"]:
                    Console.start_cycle(Level.DEBUG, f"{dir} removing existing files...")

                    for filename in os.listdir(dir):
                        filepath = f"{dir}/{filename}"

                        Console.log(Level.TRACE, f"Removing {filepath}")
                        if os.path.isfile(filepath) or os.path.islink(filepath):
                            os.unlink(filepath)

                        elif os.path.isdir(filepath):
                            shutil.rmtree(filepath)

                    Console.end_cycle()
                    return True

                elif inp_reload in ['n', "no"]:
                    Console.log(Level.DEBUG, f"Proceeding with existing files")
                    return False
                
                else:
                    Console.log(Level.ERROR, f"Invalid input. Please enter either yes or no")

        else:
            Console.start_cycle(Level.DEBUG, f"Creating {dir}")
            os.makedirs(dir)
            Console.end_cycle()
            Console.log(Level.INFO, f"Created {dir}")
            return True


    if reload_directory(f"{TMP_DIR}/bus_data"):
        Console.log(Level.DEBUG, "Downloading bus_data.zip...")
        bus_data_url = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/west_midlands/"
        wget.download(bus_data_url, out=f"{TMP_DIR}/bus_data/bus_data.zip", bar=Console._gen_bar_str)

        if Console.cr: Console.log()
        Console.log(Level.TRACE, f"Downloaded {TMP_DIR}/bus_data/bus_data.zip")
    Console.log(Level.NONE, ' ')

    if reload_directory(f"{CSV_DIR}/bus_data"):
        Console.log(Level.DEBUG, "Unzipping bus_data.zip...")
        bus_data_zip = zipfile.ZipFile(f"{TMP_DIR}/bus_data/bus_data.zip")
        for i, f in enumerate(bus_data_zip.namelist()):
            bus_data_zip.extract(f, f"{CSV_DIR}/bus_data")
            Console.log(Level.TRACE, f"Unzipped {CSV_DIR}/bus_data/{f}")
            Console.bar(Level.DEBUG, i, len(bus_data_zip.namelist())-1)
        if Console.cr: Console.log()
    Console.log(Level.NONE, ' ')

    
    if reload_directory(f"{TMP_DIR}/osm_data"):
        Console.log(Level.DEBUG, "Downloading osm_data.osm.pbf...")
        osm_data_url = "https://download.geofabrik.de/europe/united-kingdom/england-latest.osm.pbf"
        wget.download(osm_data_url, out=f"{TMP_DIR}/osm_data/osm_data.osm.pbf", bar=Console._gen_bar_str)

        if Console.cr: Console.log()
        Console.log(Level.TRACE, f"Downloaded {TMP_DIR}/osm_data/osm_data.osm.pbf")
    Console.log(Level.NONE, ' ')

    if reload_directory(f"{CSV_DIR}/osm_data"):
        Console.log(Level.DEBUG, "Generating CSV files...")
        
        csv = open(f"{CSV_DIR}/osm_data/venues.txt", 'w')
        csv.write('id,name,lat,lon,key,tag,wikidata\n')

        tag_filter = osmium.filter.TagFilter(
            ('amenity', 'pub'), ('amenity', 'cinema'), ('amenity', 'theatre'), ('amenity', 'cafe'), ('amenity', 'bicycle_rental'), 
            ('amenity', 'music_venue'), ('amenity', 'boat_rental'), ('amenity', 'bar'),
            ('tourism', 'attraction'), ('tourism', 'museum'), ('tourism', 'gallery'), ('tourism', 'theme_park'), ('tourism', 'zoo'), 
            ('tourism', 'aquarium'), 
            ('natural', 'beach'), ('natural', 'park'), ('natural', 'waterfall'), 
            ('leisure', 'park'), ('leisure', 'garden'), ('leisure', 'nature_reserve'), 
            ('historic', 'castle'), ('historic', 'fort'), ('historic', 'monument'), ('historic', 'farm'), 
            ('historic', 'archaeological_site'), ('historic', 'battlefield'), ('historic', 'ruins'), ('historic', 'city_gate'), 
            ('historic', 'building'), ('historic', 'house'), ('historic', 'aircraft'), ('historic', 'aqueduct')
        )
        total = 0

        Console.start_cycle(Level.DEBUG, "Loading osm data")
        for i, o in enumerate(osmium.FileProcessor(f"{TMP_DIR}/osm_data/osm_data.osm.pbf")\
            .with_areas()\
            .with_locations()\
            .with_filter(tag_filter)):

            if o.is_area():
                rings = MultiPolygon([
                    Polygon([(n.lat,n.lon) for n in outer if n.location.valid()])
                ] for outer in o.outer_rings())
                
                location = rings.centroid

            elif o.is_node():
                loc_str = str(o.location).split('/')
                location = Point(loc_str[1], loc_str[0])

            if o.is_area() or o.is_node():

                if 'name' in o.tags and 'car park' not in o.tags['name'].lower():
                    name = o.tags['name']
                else:
                    continue
                    
                if 'wikidata' in o.tags:
                    wikidata = f'{o.tags['wikidata']}'
                else:
                    wikidata = ''

                if 'amenity'in o.tags and o.tags['amenity'] in ['pub', 'cinema', 'theatre', 'cafe', 'bicycle_rental', 'music_venue', 'boat_rental', 'bar']:
                    key = 'amenity'
                    tag = o.tags['amenity']
                    
                elif 'tourism' in o.tags and o.tags['tourism'] in ['attraction', 'museum', 'gallery', 'theme_park', 'zoo', 'aquarium']:
                    key = 'tourism'
                    tag = o.tags['tourism']
                
                elif 'natural' in o.tags and o.tags['natural'] in ['beach', 'park', 'waterfall']:
                    key = 'natural'
                    tag = o.tags['natural']
                
                elif 'leisure' in o.tags and o.tags['leisure'] in ['park', 'garden', 'nature_reserve']:
                    key = 'leisure'
                    tag = o.tags['leisure']
                
                elif 'historic' in o.tags and o.tags['historic'] in ['castle', 'fort', 'monument', 'farm', 'archaeological_site', 'battlefield', 'ruins', 'city_gate', 'building', 'house', 'aircraft', 'aqueduct']:
                    key = 'historic'
                    tag = o.tags['historic']
                
                if '"' in name:
                    name = f"'{name}'"
                else:
                    name = f'"{name}"'

                line = f'{o.id},{name},{location.x},{location.y},{key},{tag},{wikidata}'
                csv.write(f"{line}\r\n")
                total += 1
                Console.log(Level.TRACE, line)
        Console.end_cycle()
        Console.log(Level.TRACE, f"Generated {total} results")

        csv.close()

    Console.log(Level.NONE, ' ')

# endregion download data


    Database.auth()
    tqd = QueryData()

    Console.log(Level.NONE)
    Console.log(Level.INFO, "Ensuring neo4j constraints and indexes...")

    Console.log(Level.DEBUG, "Ensuring constraints")
# region constraints

    constraints = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Agency) REQUIRE a.agency_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Route) REQUIRE r.route_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Calendar) REQUIRE c.service_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Stop) REQUIRE s.stop_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (v:Venue) REQUIRE v.venue_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Trip) REQUIRE t.trip_id IS UNIQUE"
    ]

    Console.bar(0,len(constraints))
    for i, contstraint in enumerate(constraints):
        tqd += Database.exec(contstraint)
        Console.bar(i+1,len(constraints))
    if Console.cr: Console.log()

# endregion constraints


    Console.log(Level.DEBUG, "Ensuring indexes")
# region indexes

    indexes = [
        "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[p:PRECEDES]-() ON p.timeTraveled",
        "CREATE RANGE INDEX IF NOT EXISTS FOR ()-[h:HAS]-() ON h.distWalked",
        "CREATE POINT INDEX IF NOT EXISTS FOR (v:Venue) ON v.location",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (v:Venue) ON v.category",
        "CREATE POINT INDEX IF NOT EXISTS FOR (s:Stop) ON s.location",
        "CREATE TEXT INDEX IF NOT EXISTS FOR (s:Stop) ON s.stop_name",
        "CREATE TEXT INDEX IF NOT EXISTS FOR (r:Route) ON r.route_name",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (st:StopTime) ON st.seq",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (st:StopTime) ON st.arrival_time",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (st:StopTime) ON st.departure_time",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.`0`",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.`1`",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.`2`",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.`3`",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.`4`",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.`5`",
        "CREATE RANGE INDEX IF NOT EXISTS FOR (c:Calendar) ON c.`6`"
    ]

    Console.bar(0,len(indexes))
    for i, index in enumerate(indexes):
        tqd += Database.exec(index)
        Console.bar(i+1,len(indexes))
    if Console.cr: Console.log()

# endregion indexes


    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, "Loading and updating service dates...")
# region calendars

    Console.log(Level.DEBUG, "Removing unused calendars")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/calendar.txt' AS calendar
            WITH collect(DISTINCT calendar.service_id) AS service_ids

            MATCH (c:Calendar)
            WHERE NOT c.service_id IN service_ids
        """,
        """
            DETACH DELETE c
        """, 400
    )

    Console.log(Level.DEBUG, "Detaching trips that have wrong calendar attached")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/trips.txt' AS trip
            WITH trip
            
            MATCH (t:Trip {trip_id: trip.trip_id})-[rel:RUNS_ON]->(c:Calendar)
            WHERE NOT c.service_id = trip.service_id
        """,
        """
            DELETE rel
        """, 40000
    )

    Console.log(Level.DEBUG, "Creating or updating calendars")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/calendar.txt' AS calendar
            WITH calendar
        """,
        """
            MERGE (c:Calendar {service_id: toString(calendar.service_id)})
            SET c['0'] = toBoolean(toInteger(calendar.monday)),
                c['1'] = toBoolean(toInteger(calendar.tuesday)),
                c['2'] = toBoolean(toInteger(calendar.wednesday)),
                c['3'] = toBoolean(toInteger(calendar.thursday)),
                c['4'] = toBoolean(toInteger(calendar.friday)),
                c['5'] = toBoolean(toInteger(calendar.saturday)),
                c['6'] = toBoolean(toInteger(calendar.sunday)),
                c.start_date = date(calendar.start_date),
                c.end_date = date(calendar.end_date);
        """, 400
    )

# endregion calendars


    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, "Loading and updating agencies...")
# region agencies

    Console.log(Level.DEBUG, "Removing unused agencies")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/agency.txt' AS agency
            WITH collect(DISTINCT agency.agency_id) AS agency_ids

            MATCH (a:Agency)
            WHERE NOT a.agency_id IN agency_ids
        """,
        """
            DETACH DELETE a
        """, 400
    )

    Console.log(Level.DEBUG, "Detaching routes that have wrong agency attached")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/routes.txt' AS route
            WITH route

            MATCH (a:Agency)-[rel:OPERATES]->(:Route {route_id: route.route_id})
            WHERE NOT a.agency_id = route.agency_id
        """,
        """
            DELETE rel
        """, 40000
    )

    Console.log(Level.DEBUG, "Creating or updating agencies")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/agency.txt' AS agency
            WITH agency
        """,
        """
            MERGE (a:Agency {agency_id: toString(agency.agency_id)})
            SET a.agency_name = toString(agency.agency_name)
        """, 40000
    )

# endregion agencies


    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, "Loading and updating routes...")
# region routes

    Console.log(Level.DEBUG, "Removing unused routes")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/routes.txt' AS route
            WITH collect(DISTINCT route.route_id) AS route_ids

            MATCH (r:Route)
            WHERE NOT r.route_id IN route_ids
        """,
        """
            DETACH DELETE r
        """, 4000
    )

    Console.log(Level.DEBUG, "Creating or updating routes")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/routes.txt' AS route
            WITH route
        """,
        """
            MERGE (r:Route {route_id: toString(route.route_id)})
            SET r.route_name = toString(route.route_short_name)

            WITH route, r

            MATCH (a:Agency {agency_id: route.agency_id})
            MERGE (a)-[:OPERATES]->(r)
        """, 4000
    )

# endregion routes


    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, "Loading and updating trips...")
# region trips

    Console.log(Level.DEBUG, "Removing unused trips")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/trips.txt' AS trip
            WITH collect(DISTINCT trip.trip_id) AS trip_ids

            MATCH (t:Trip)
            WHERE NOT t.trip_id IN trip_ids
        """,
        """
            DETACH DELETE t
        """, 4000
    )

    Console.log(Level.DEBUG, "Creating or updating trips")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/trips.txt' AS trip
            WITH trip
        """,
        """
            MERGE (t:Trip {trip_id: toString(trip.trip_id)})
            SET t.headsign = toString(trip.trip_headsign),
                t.accessible = toBoolean(toString(trip.wheelchair_accessible))

            WITH trip, t

            MATCH (r:Route {route_id: trip.route_id})
            MERGE (t)-[:BELONGS_TO]->(r)

            WITH trip, t

            MATCH (c:Calendar {service_id: trip.service_id})
            MERGE (t)-[:RUNS_ON]->(c)
        """, 4000
    )
    
# endregion trips


    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, "Loading and updating stops...")
# region stops

    Console.log(Level.DEBUG, "Removing unused stops")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stops.txt' AS stop
            WITH collect(DISTINCT stop.stop_id) AS stop_ids

            MATCH (s:Stop)
            WHERE NOT s.stop_id IN stop_ids
            AND NOT (s)<-[:CHILD_OF]-(:Stop)
        """,
        """
            DETACH DELETE s
        """, 16000
    )

    Console.log(Level.DEBUG, "Removing obsolete parent stops")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stops.txt' AS stop
            WITH stop
            
            WHERE stop.location_type IS NOT NULL AND toInteger(stop.location_type) < 2

            ORDER BY stop.stop_id
            WITH apoc.map.groupByMulti(collect(stop), 'parent_station') AS cs, collect(stop) AS stops
            UNWIND stops AS stop

            WITH cs, stop
            WHERE toInteger(stop.location_type) = 1
            WITH stop, cs[stop.stop_id] AS cs

            UNWIND cs AS s
            WITH apoc.map.groupBy(collect(stop {
                location:point({latitude:toFloat(stop.stop_lat),longitude:toFloat(stop.stop_lon)}),
                stop_id:s.stop_id
            }), 'stop_id') AS pl

            MATCH (s:Stop)-[:CHILD_OF]->(p:Stop)

            ORDER BY p.stop_id, s.stop_id
            WITH p, 
                p.stop_id AS parent_id, 
                apoc.util.md5(collect(s.stop_id)) AS hash_id, 
                pl[toString(s.stop_id)].location AS pl, 
                collect(s) AS gs
            WITH p, CASE WHEN pl IS NOT NULL THEN pl ELSE point({
                latitude: reduce(latSum = 0, s IN gs | latSum + s.location.y) / size(gs),
                longitude: reduce(lngSum = 0, s IN gs | lngSum + s.location.x) / size(gs)
            }) END AS pl, parent_id, hash_id
            WHERE (NOT parent_id = hash_id)
            OR (NOT pl = p.location)
        """,
        """
            DETACH DELETE p
        """, 16000
    )
    
    Console.log(Level.DEBUG, "Creating or updating stops")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stops.txt' AS stop
            WITH stop

            WHERE stop.location_type IS NULL OR toInteger(stop.location_type) = 0
        """,
        """
            MERGE(s:Stop {stop_id: toString(stop.stop_id)})
            SET s.stop_name = toString(stop.stop_name),
                s.accessible = toBoolean(toInteger(stop.wheelchair_boarding)),
                s.location = point({
                    latitude: toFloat(stop.stop_lat), 
                    longitude: toFloat(stop.stop_lon)
                })
        """, 16000
    )

    Console.log(Level.DEBUG, "Creating or updating parent stops")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stops.txt' AS stop
            WITH stop
            
            WHERE stop.location_type IS NOT NULL AND toInteger(stop.location_type) < 2

            ORDER BY stop.stop_id
            WITH apoc.map.groupByMulti(collect(stop), 'parent_station') AS cs, collect(stop) AS stops
            UNWIND stops AS stop

            WITH cs, stop
            WHERE toInteger(stop.location_type) = 1
            WITH stop, cs[stop.stop_id] AS cs

            UNWIND cs AS s
            ORDER BY stop, s.stop_id
            WITH stop, collect(DISTINCT s.stop_id) AS hash, collect(s) AS cs
            WITH stop, apoc.util.md5(hash) AS hash, cs
        """,
        """
            MERGE (p:Stop {stop_id: toString(hash)})
            SET p.stop_name = toString(stop.stop_name),
                p.accessible = toBoolean(toInteger(stop.wheelchair_boarding)),
                p.location = point({
                    latitude: toFloat(stop.stop_lat),
                    longitude: toFloat(stop.stop_lon)
                })
            WITH p, cs
            UNWIND cs AS s
            MATCH (c:Stop {stop_id: s.stop_id})
            MERGE (c)-[:CHILD_OF]->(p)
        """, 16000
    )

    Console.log(Level.DEBUG, "Generating parents for nearby stops...")
    Console.start_cycle(Level.DEBUG, "Processing parents for nearby stops...")
    Console.log(Level.TRACE, "Creating groups from stops less then 80m apart, with a similar name")
    response = Database._exec(
        """
            MATCH (s1:Stop)
            WHERE NOT (s1)-[:CHILD_OF]-(:Stop)
            WITH s1
            MATCH (s2:Stop)
            WHERE NOT (s2)-[:CHILD_OF]-(:Stop)
            AND elementId(s1) <> elementId(s2)
            AND substring(s1.stop_id, 0, 4) =
                substring(s2.stop_id, 0, 4)
            AND point.distance(s1.location, s2.location) < 80
            AND apoc.text.jaroWinklerDistance(
                    apoc.text.replace(s1.stop_name,'^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$',''),
                    apoc.text.replace(s2.stop_name,'^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$','')
            ) < .2

            WITH s1 {stop_name:s1.stop_name, stop_id:s1.stop_id, location:s1.location}, collect(s2 {stop_name:s2.stop_name, stop_id:s2.stop_id, location:s2.location}) AS s2s
            WITH s1 AS fs, ([s1] + s2s) AS gs

            UNWIND gs AS s
            ORDER BY s.stop_id
            WITH fs, collect(s) AS gs
            WITH DISTINCT gs
            WITH gs[0] AS fs, gs

            RETURN count(gs) AS cgs1, collect(gs) AS ggs
        """
    )
    cgs1, ggs = response.single()
    tqd += QueryData(response.consume())

    if cgs1 > 0:
        Console.log(Level.TRACE, "Daisy chaining groups...")

        chain_count=1
        while True:
            response = Database._exec(
                """
                    UNWIND $ggs AS gs
                    UNWIND gs AS s
                    WITH s, collect(gs) AS gs
                    WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
                    RETURN count(gs) AS cgs2, collect(gs) AS ggs
                """,
                ggs=ggs
            )
            cgs2, ggs = response.single()
            tqd += QueryData(response.consume())

            Console.log(Level.TRACE, chain_count, ': ', cgs1, ', ', cgs2)

            if cgs1 == cgs2:
                break
            else:
                cgs1 = cgs2
                chain_count += 1

        Console.log(Level.TRACE, "Creating parents from the generated groups")
        tqd += Database.exec(
            """
                UNWIND $ggs AS gs
                UNWIND gs AS s
                ORDER BY s.stop_id
                WITH gs, collect(s) AS gso, collect(s.stop_id) AS hash
                WITH DISTINCT gso AS gs, apoc.util.md5(hash) AS hash
                
                WITH gs,
                    REDUCE(latSum = 0, s IN gs | latSum + s.location.y) / size(gs) AS lat,
                    REDUCE(latSum = 0, s IN gs | latSum + s.location.x) / size(gs) AS lng,
                    apoc.text.replace(gs[0].stop_name,'^\\d+ *?(?=[A-Za-z])| *?[Aa]lighting [Oo]nly$| *?[Aa]rrivals?$| *?\\[.*?\\]$| *?\\(.*?\\)$|(?<=[A-Za-z])[\\. ]*?\\d+$','') AS stop_name, hash

                MERGE (p:Stop {stop_id: toString(hash)})
                SET p.stop_name = toString(stop_name),
                    p.accessible = gs[0].accessible,
                    p.location = point({
                        latitude: toFloat(lat), 
                        longitude: toFloat(lng)
                    })
                WITH p, gs
                UNWIND gs AS g
                MATCH (s:Stop {stop_id: g.stop_id})
                MERGE (p)<-[:CHILD_OF]-(s)
            """, ggs=ggs
        )

    else:
        Console.log(Level.TRACE, "No stops could be grouped")
    Console.end_cycle()

    Console.log(Level.DEBUG, "Removing relationships for stops that are more then 1000m apart")
    tqd += Database.exec_loop(
        """
            MATCH (s1:Stop)-[n:NEARBY]->(s2:Stop) 
            WITH s1.location AS sl1, n, s2.location AS sl2
            WHERE point.distance(sl1, sl2) > 1000 
        """,
        """
            DELETE n
        """, 8000
    )
    
    Console.log(Level.DEBUG, "Creating relationships for stops that are less then 1000m apart")
    tqd += Database.exec_loop(
        """
            MATCH (s1:Stop)
            WHERE (NOT (s1)-[:CHILD_OF]-(:Stop) OR (:Stop)-[:CHILD_OF]->(s1))

            MATCH (s2:Stop)
            WHERE (NOT (s2)-[:CHILD_OF]-(:Stop) OR (:Stop)-[:CHILD_OF]->(s2))
            AND elementId(s2) < elementId(s1)
            AND point.distance(s1.location, s2.location) < 1000

            WITH s1, s2, point.distance(s1.location, s2.location) AS dist
            ORDER BY s1, dist
            WITH s1, collect(s2 {s2, dist})[..10] AS s2s
            UNWIND s2s AS s2
            WITH s1, s2.s2 AS s2, s2.dist AS dist
        """,
        """
            MERGE (s1)-[n:NEARBY]-(s2)
            SET n.distWalked = dist
        """, 8000
    )

# endregion stops


    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, "Loading and updating stoptimes...")
# region stoptimes

    Console.log(Level.DEBUG, "Removing unused stoptimes")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stop_times.txt' AS stoptime
            WITH stoptime.trip_id AS trip_id, collect(toInteger(stoptime.stop_sequence)) AS seqs
            WITH collect(DISTINCT trip_id) AS trip_ids, apoc.map.groupBy(collect({trip_id:trip_id, seqs:seqs}), 'trip_id') AS seqs_map

            MATCH (st:StopTime)
            OPTIONAL MATCH (st)-[:PART_OF]->(t:Trip)
            WITH st, t, seqs_map
            WHERE (NOT (st)-[:PART_OF]->(:Trip))
               OR (NOT (st)<-[:TIME_TABLED]-(:Stop))
               OR (NOT t.trip_id IN trip_ids)
               OR (NOT st.seq IN seqs_map[toString(t.trip_id)].seqs)
        """,
        """
            DETACH DELETE st
        """, 80000
    )

    Console.log(Level.DEBUG, "Creating or updating stop times")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stop_times.txt' AS stoptime
            WITH stoptime
        """,
        """
            MATCH(t:Trip {trip_id: stoptime.trip_id}), (s:Stop {stop_id: stoptime.stop_id})

            WITH t, s, stoptime.stop_sequence AS seq, 
                split(stoptime.departure_time, ':') AS departureParts,
                split(stoptime.arrival_time, ':') AS arrivalParts

            MERGE (t)<-[:PART_OF]-(st:StopTime {seq: toInteger(seq)})<-[:TIME_TABLED]-(s)
            SET st.arrival_time = 
                    (toInteger(arrivalParts[0]) * 3600) + 
                    (toInteger(arrivalParts[1]) * 60) +
                    (toInteger(arrivalParts[2])),
                st.departure_time = 
                    (toInteger(departureParts[0]) * 3600) + 
                    (toInteger(departureParts[1]) * 60) +
                    (toInteger(departureParts[2]))
        """, 80000
    )

    Console.log(Level.DEBUG, "Linking all stop times that precede one another")
    tqd += Database.exec_loop(
        """
            MATCH (st:StopTime)-[:PART_OF]->(t:Trip)
            ORDER BY t.trip_id, st.seq
            WITH t.trip_id AS trip_id, collect(st) AS sts
            UNWIND range(0, size(sts) - 1) AS i
            WITH trip_id, sts[i] AS st1, sts[i+1] AS st2
            WHERE st2 IS NOT NULL
            WITH st1, st2
        """,
        """
            MERGE (st1)-[pre:PRECEDES]->(st2)
            SET pre.timeTraveled = st2.departure_time - st1.arrival_time
        """, 80000
    )

# endregion stoptimes


    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, "Loading and updating venues...")
# region venues

    Console.log(Level.DEBUG, "Removing unused venues")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/venues.txt' AS venue
            WITH collect(venue.id) AS venue_ids

            MATCH (v:Venue)
            WHERE NOT v.venue_id IN venue_ids
        """,
        """
            DETACH DELETE v
        """, 8000
    )

    Console.log(Level.DEBUG, "Removing relationships for venues more then 1km from stop")
    tqd += Database.exec_loop(
        """
            MATCH (v:Venue)<-[h:HAS]-(s:Stop)
            WHERE point.distance(v.location, s.location) > 1000
        """,
        """
            DELETE h
        """, 8000
    )
    
    Console.log(Level.DEBUG, "Creating or updating venues")
    tqd += Database.exec_loop(
        """
            LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/venues.txt' AS venue
            WITH venue
        """,
        """
            MATCH (s:Stop)
            WHERE point.distance(s.location, point({
                latitude: toFloat(venue.lat),
                longitude: toFloat(venue.lon)
            })) < 1000

            MERGE (v:Venue {venue_id: toString(venue.id)})
            SET v.venue_name = venue.name,
                v.category = [venue.key, venue.tag],
                v.wikidata = toString(venue.wikidata),
                v.location = point({
                    latitude: toFloat(venue.lat),
                    longitude: toFloat(venue.lon)
                })

            MERGE (s)-[h:HAS]->(v)
            SET h.distWalked = point.distance(s.location, v.location)
        """, 8000
    )

# endregion venues

    Console.log(Level.NONE, ' ')
    Console.log(Level.INFO, tqd)

except KeyboardInterrupt:
    if Console.cr: Console.log()
    Console.log(Level.NONE, ' ')
    Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")
    os._exit(1)

except Exception as e:
    if Console.cr: Console.log()
    Console.log(Level.NONE, ' ')
    Console.log(Level.FATAL, e)
    os._exit(1)
