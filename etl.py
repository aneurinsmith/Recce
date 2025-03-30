
import re
import sys
import os
import math
import wget
import zipfile
import osmium
import shutil
import argparse
from shapely.geometry import MultiPolygon, Polygon, Point
from enum import Enum
from neo4j import GraphDatabase
from getpass import getpass, getuser


CSV_DIR = "/var/lib/neo4j/import/recce"
TMP_DIR = "/tmp/recce"
MIN_LOG_LVL = 1

class QueryData:
    label_name: str = ''

    nodes_created: int = 0
    nodes_deleted: int = 0
    relationships_created: int = 0
    relationships_deleted: int = 0
    labels_added: int = 0
    labels_removed: int = 0
    properties_set: int = 0
    indexes_added: int = 0
    indexes_removed: int = 0
    constraints_added: int = 0
    constraints_removed: int = 0
    execution_time: int = 0

    def __iadd__(self, other):
        self.nodes_created += other.nodes_created
        self.nodes_deleted += other.nodes_deleted
        self.relationships_created += other.relationships_created
        self.relationships_deleted += other.relationships_deleted
        self.labels_added += other.labels_added
        self.labels_removed += other.labels_removed
        self.properties_set += other.properties_set
        self.indexes_added += other.indexes_added
        self.indexes_removed += other.indexes_removed
        self.constraints_added += other.constraints_added
        self.constraints_removed += other.constraints_removed
        self.execution_time += other.execution_time

        return self
    
    def __gt__(self, other):
        if isinstance(other, int):
            return \
                self.nodes_created > other or \
                self.nodes_deleted > other or \
                self.relationships_created > other or \
                self.relationships_deleted > other or \
                self.labels_added > other or \
                self.labels_removed > other or \
                self.properties_set > other or \
                self.indexes_added > other or \
                self.indexes_removed > other or \
                self.constraints_added > other or \
                self.constraints_removed > other
        return NotImplemented
    
    def __str__(self):
        result = ''
        if self.nodes_created > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.nodes_created} node{'' if self.nodes_created == 1 else 's'} created\n"
        if self.nodes_deleted > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.nodes_deleted} node{'' if self.nodes_deleted == 1 else 's'} deleted\n"
        if self.relationships_created > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.relationships_created} relationship{'' if self.relationships_created == 1 else 's'} created\n"
        if self.relationships_deleted > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.relationships_deleted} relationship{'' if self.relationships_deleted == 1 else 's'} deleted\n"
        if self.labels_added > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.labels_added} label{'' if self.labels_added == 1 else 's'} added\n"
        if self.labels_removed > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.labels_removed} label{'' if self.labels_removed == 1 else 's'} removed\n"
        if self.properties_set > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.properties_set} propert{'y' if self.properties_set == 1 else 'ies'} set\n"
        if self.indexes_added > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.indexes_added} index{'' if self.indexes_added == 1 else 'es'} added\n"
        if self.indexes_removed > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.indexes_removed} index{'' if self.indexes_removed == 1 else 'es'} removed\n"
        if self.constraints_added > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.constraints_added} constraint{'' if self.constraints_added == 1 else 's'} added\n"
        if self.constraints_removed > 0: result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0m{self.constraints_removed} constraint{'' if self.constraints_removed == 1 else 's'} removed\n"

        result += f"\033[1m{self.label_name+' ' if len(self.label_name) > 0 else ''}\033[0mcompleted after {self.execution_time}ms"
        return result

class Level(Enum):
    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARN = 3
    ERROR = 4
    FATAL = 5
    NONE = 6

class Console:
    cr = False

    def gen_bar(*args: tuple):

        lvl = Level.INFO
        if args and isinstance(args[0], Level):
            lvl = args[0] if len(args) > 0 else Level.INFO
            current = args[1] if len(args) > 1 else 0
            total = args[2] if len(args) > 2 else 100
            width = args[3] if len(args) > 3 else 80
        else:
            current = args[0] if len(args) > 0 else 0
            total = args[1] if len(args) > 1 else 100
            width = args[2] if len(args) > 2 else 80

        available_space = width - len(str(current)) - len(str(total))
        completed_space = int(math.floor(float(current) / total * available_space))

        percent = round((current / total) * 100)
        Console.cr = True
        return Console.gen_str(lvl, 
            f"[ {current} / {total} ] "
            f"{'■'*completed_space}{'='*(available_space-completed_space)} "
            f"{percent}% "
        ) 
    
    def gen_str(*args: tuple):
        lvl_str = ''
        if len(args) > 0 and type(args[0]) == Level:

            if args[0] == Level.TRACE:
                lvl_str = '\033[90m[TRACE]\033[0m\t '
            elif args[0] == Level.DEBUG:
                lvl_str = '\033[97m[DEBUG]\033[0m\t '
            elif args[0] == Level.INFO:
                lvl_str = '\033[97;46m [INFO]\033[0m\t '
            elif args[0] == Level.WARN:
                lvl_str = '\033[43;30m [WARN]\033[0m\t '
            elif args[0] == Level.ERROR:
                lvl_str = '\033[97;101m[ERROR]\033[0m\t '
            elif args[0] == Level.FATAL:
                lvl_str = '\033[97;41m[FATAL]\033[0m\t '

            args = args[1:]

        return f"{lvl_str}{''.join(str(msg).replace('\n', '\n\t ') for msg in args)}\033[0m"
    
    def isloggable(lvl: Level):
        if lvl.value >= MIN_LOG_LVL:
            return True
        else:
            return False

    def log(*args: tuple, end="\n\r"):
        lvl = Level.NONE
        if args and isinstance(args[0], Level):
            lvl = args[0]

        if Console.isloggable(lvl):
            print(Console.gen_str(*args), end=end)
            if len(args) > 0 and isinstance(args[-1], str) and args[-1].endswith('\r'):
                Console.cr = True
            else:
                Console.cr = False
        
        if os.path.isdir(f"{TMP_DIR}/log"):
            file = open(f"{TMP_DIR}/log/trace.txt", 'a')
            file.write(Console.gen_str(*args) + '\n')

    def bar(*args):
        lvl = Level.INFO
        if args and isinstance(args[0], Level):
            lvl = args[0]

        if Console.isloggable(lvl):
            print(Console.gen_bar(*args), end='\r')
            Console.cr = True

    def inp(msg: str, isPrivate: bool = False, default_value = None):
        full_msg = f"\033[95m[INPUT]\033[0m\t {msg}"
        if default_value is not None:
            full_msg += f" [{default_value}]"
        full_msg += ": "

        Console.cr = True
        if isPrivate:
            inp_res = getpass(full_msg) or default_value
        else:
            inp_res = input(full_msg) or default_value
        return inp_res


def init_directory(path): 
    if os.path.isdir(path):
        if not os.listdir(path) == []:
            Console.log(Level.DEBUG, f"Removing contents of {path}")
            for filename in os.listdir(path):
                filepath = f"{path}/{filename}"
                try:
                    if os.path.isfile(filepath) or os.path.islink(filepath):
                        Console.log(Level.TRACE, f"Removing {filepath}")
                        os.unlink(filepath)
                    elif os.path.isdir(filepath):
                        Console.log(Level.TRACE, f"Removing {filepath}")
                        shutil.rmtree(filepath)
                except Exception as e:
                    Console.log(Level.FATAL, f"{e.strerror}: Failed to delete {filepath}")
                    exit(1)
    else:
        try:
            Console.log(Level.TRACE, f"Creating {path}")
            os.makedirs(path)
        except Exception as e:
            Console.log(Level.FATAL, f"{e.strerror}: Failed to create {path}")
            exit(1)

def download_files():
    reload = "no"
    if os.path.isdir(f"{CSV_DIR}/bus_data"):
        lc = 0
        while True:
            in_reload = Console.inp("Bus data already previously downloaded. Would you like to redownload it?", False, reload)
            
            if in_reload in ['y', "yes"]:
                Console.log(Level.TRACE, "Proceeding with redownloaded bus data...")
                reload = in_reload
                break

            elif in_reload in ['n', "no"]:
                Console.log(Level.TRACE, "Proceeding without redownloading bus data...")
                reload = in_reload
                break

            else:
                Console.log(Level.ERROR, f"Invalid input \"{in_reload}\". Please enter either yes or no")

            lc+=1
            if lc > 10:
                Console.log(Level.FATAL, f"Failed too many times. Exiting script.")
                exit(1)
    
    if reload in ['y', "yes"]:

        init_directory(f"{CSV_DIR}/bus_data")
        init_directory(f"{TMP_DIR}/bus_data")

        try:
            Console.log(Level.DEBUG, "Downloading bus_data.zip...")
            bus_data_url = "https://data.bus-data.dft.gov.uk/timetable/download/gtfs-file/west_midlands/"

            wget_progress_bar = Console.gen_bar

            wget.download(bus_data_url, out=f"{TMP_DIR}/bus_data/bus_data.zip", bar=wget_progress_bar)

            if Console.cr == True: print()
            Console.log(Level.TRACE, f"bus_data downloaded to {TMP_DIR}/bus_data/bus_data.zip")
            
            Console.log(Level.DEBUG, "Unzipping bus_data.zip...")
            bus_data_zip = zipfile.ZipFile(f"{TMP_DIR}/bus_data/bus_data.zip")
            for f in bus_data_zip.namelist():
                bus_data_zip.extract(f, f"{CSV_DIR}/bus_data")
                Console.log(Level.TRACE, f"Unzipped {CSV_DIR}/bus_data/{f}")

        except Exception as e:
            Console.log(Level.FATAL, f"{e.strerror}: Failed to download bus data")
            exit(1)

    reload = "no"
    if os.path.isdir(f"{CSV_DIR}/osm_data"):
        lc = 0
        while True:
            in_reload = Console.inp("OSM data already previously downloaded. Would you like to redownload it?", False, reload)
            
            if in_reload in ['y', "yes"]:
                Console.log(Level.TRACE, "Proceeding with redownloaded osm data...")
                reload = in_reload
                break

                
            elif in_reload in ['n', "no"]:
                Console.log(Level.TRACE, "Proceeding without redownloading osm data...")
                reload = in_reload
                break
                
            else:
                Console.log(Level.ERROR, f"Invalid input \"{in_reload}\". Please enter either yes or no")

            lc+=1
            if lc > 10:
                Console.log(Level.FATAL, f"Failed too many times. Exiting script.")
                exit(1)
    
    if reload in ['y', "yes"]:

        init_directory(f"{CSV_DIR}/osm_data")
        init_directory(f"{TMP_DIR}/osm_data")

        try:
            Console.log(Level.DEBUG, "Downloading osm_data.osm.pbf...")
            osm_data_url = "https://download.geofabrik.de/europe/united-kingdom/england/staffordshire-latest.osm.pbf"

            wget_progress_bar = Console.gen_bar

            wget.download(osm_data_url, out=f"{TMP_DIR}/osm_data/osm_data.osm.pbf", bar=wget_progress_bar)

            if Console.cr == True: print()
            Console.log(Level.TRACE, f"osm_data downloaded to {TMP_DIR}/osm_data/osm_data.osm.pbf")
            
            Console.log(Level.DEBUG, "Generating CSV files...")

        except Exception as e:
            Console.log(Level.FATAL, f"{e.strerror}: Failed to download osm data")
            exit(1)
        

        csv = open(f"{CSV_DIR}/osm_data/venues.txt", 'w')
        csv.write('id,name,lat,lon,key,tag,wikidata\n')

        for o in osmium.FileProcessor(f"{TMP_DIR}/osm_data/osm_data.osm.pbf")\
            .with_areas()\
            .with_locations()\
            .with_filter(osmium.filter.TagFilter(
                ('amenity', 'pub'), ('amenity', 'cinema'), ('amenity', 'theatre'), ('amenity', 'cafe'), ('amenity', 'bicycle_rental'), 
                ('amenity', 'music_venue'), ('amenity', 'boat_rental'), ('amenity', 'bar'),
                ('tourism', 'attraction'), ('tourism', 'museum'), ('tourism', 'gallery'), ('tourism', 'theme_park'), ('tourism', 'zoo'), 
                ('tourism', 'aquarium'), 
                ('natural', 'beach'), ('natural', 'park'), ('natural', 'waterfall'), 
                ('leisure', 'park'), ('leisure', 'garden'), ('leisure', 'nature_reserve'), 
                ('historic', 'castle'), ('historic', 'fort'), ('historic', 'monument'), ('historic', 'farm'), 
                ('historic', 'archaeological_site'), ('historic', 'battlefield'), ('historic', 'ruins'), ('historic', 'city_gate'), 
                ('historic', 'building'), ('historic', 'house'), ('historic', 'aircraft'), ('historic', 'aqueduct'), 
            )):

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
                    wikidata = o.tags['wikidata']
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
                
                line = f'{o.id},"{name}",{location.x},{location.y},"{key}","{tag}",{wikidata}'
                csv.write(f"{line}\r\n")
                Console.log(Level.TRACE, line)

        csv.close()


def execute_query(session, query: str, **kwargs):
    msg_str = "Executing cypher query"
    if len(kwargs) > 0: msg_str += " with params " + str([', '.join(f"{key}={value}" for key, value in kwargs.items())])
    Console.log(Level.TRACE, f"{msg_str}: \n", re.sub(r'^ +', '', query, flags=re.MULTILINE))
    result = session.run(query, kwargs)
    return result

def authenticate_neo4j():
    while True:
        username = Console.inp("Enter Neo4j Username", False, getuser())
        password = Console.inp("Enter Neo4j Password", True)

        driver = GraphDatabase.driver("bolt+s://neo4j.aneur.info", auth=(username, password))
        try:
            session = driver.session(database="recce")
            session.run("RETURN 1")
            Console.log(Level.TRACE, "Authenticated successfully")
            return driver, session
        
        except Exception:
            Console.log(Level.ERROR, "Authentication failed. Please try again")

def create_constraint(session, label, value) -> QueryData:
    result = execute_query(session, f"CREATE CONSTRAINT IF NOT EXISTS FOR (l:{label}) REQUIRE l.{value} IS UNIQUE").consume()
    
    qd = QueryData()
    qd.constraints_added = result.counters.constraints_added
    qd.execution_time = result.result_available_after
    
    Console.log(Level.TRACE, 
        f"{qd.constraints_added} constraints added for "
        f"\033[1m(:{label})\033[0m on \033[1m{value}\033[0m "
        f"after {qd.execution_time}ms"
    )
    
    return qd

def create_index(session, label, value, type = "RANGE", isRelationship = False) -> QueryData:

    if isRelationship:
        label = f"()-[l:{label.upper()}]-()"

    else:
        label = f"(l:{label})"

    result = execute_query(session, f"CREATE {type} INDEX IF NOT EXISTS FOR {label} ON l.{value}").consume()

    qd = QueryData()
    qd.indexes_added = result.counters.indexes_added
    qd.execution_time = result.result_available_after
    
    Console.log(Level.TRACE, 
        f"{qd.indexes_added} index{'es' if qd.indexes_added == 0 or qd.indexes_added > 1 else ''} added for "
        f"\033[1m(:{label})\033[0m on \033[1m{value}\033[0m "
        f"after {qd.execution_time}ms"
    )

    return qd


def load_calendars(session) -> QueryData:
    qd = QueryData()

    Console.log(Level.DEBUG, "Removing unused calendars")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/calendar.txt' AS calendar
        WITH collect(DISTINCT calendar.service_id) AS service_ids

        MATCH (c:Calendar)
        WHERE NOT c.service_id IN service_ids

        DETACH DELETE c
        """
    ).consume()

    qd.nodes_deleted += result.counters.nodes_deleted
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after
    
    Console.log(Level.DEBUG, "Removing relationships for calendars that are wrongly connected")
    result = execute_query(session,
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/trips.txt' AS trip
        WITH trip

        MATCH (t:Trip {trip_id: trip.trip_id})-[rel:RUNS_ON]->(c:Calendar)
        WHERE NOT c.service_id = trip.service_id

        DELETE rel
        """
    ).consume()
    
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after
    
    Console.log(Level.DEBUG, "Creating or updating calendars")
    result = execute_query(session,
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/calendar.txt' AS row
        WITH row

        MERGE (c:Calendar {service_id: toString(row.service_id)})
        SET c['0'] = toBoolean(toInteger(row.monday)),
            c['1'] = toBoolean(toInteger(row.tuesday)),
            c['2'] = toBoolean(toInteger(row.wednesday)),
            c['3'] = toBoolean(toInteger(row.thursday)),
            c['4'] = toBoolean(toInteger(row.friday)),
            c['5'] = toBoolean(toInteger(row.saturday)),
            c['6'] = toBoolean(toInteger(row.sunday)),
            c.start_date = date(row.start_date),
            c.end_date = date(row.end_date);
        """
    ).consume()
    
    qd.labels_added = result.counters.labels_added
    qd.properties_set = result.counters.properties_set
    qd.nodes_created = result.counters.nodes_created
    qd.execution_time += result.result_available_after

    return qd

def load_agencies(session) -> QueryData:
    qd = QueryData()
    
    Console.log(Level.DEBUG, "Removing unused agencies")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/agency.txt' AS agency
        WITH collect(DISTINCT agency.agency_id) AS agency_ids

        MATCH (a:Agency)
        WHERE NOT a.agency_id IN agency_ids

        DETACH DELETE a
        """
    ).consume()
    
    qd.nodes_deleted += result.counters.nodes_deleted
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after
    
    Console.log(Level.DEBUG, "Removing relationships for agencies that are wrongly connected")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/routes.txt' AS route
        WITH route

        MATCH (a:Agency)-[rel:OPERATES]->(:Route {route_id: route.route_id})
        WHERE NOT a.agency_id = route.agency_id

        DELETE rel
        """
    ).consume()
    
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after
    
    Console.log(Level.DEBUG, "Creating or updating agencies")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/agency.txt' AS agency
        WITH agency

        MERGE (a:Agency {agency_id: toString(agency.agency_id)})
        SET a.agency_name = toString(agency.agency_name)
        """
    ).consume()

    qd.labels_added = result.counters.labels_added
    qd.properties_set = result.counters.properties_set
    qd.nodes_created = result.counters.nodes_created
    qd.execution_time += result.result_available_after

    return qd

def load_routes(session) -> QueryData:
    qd = QueryData()
    
    Console.log(Level.DEBUG, "Removing unused routes")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/routes.txt' AS route
        WITH collect(DISTINCT route.route_id) AS route_ids

        MATCH (r:Route)
        WHERE NOT r.route_id IN route_ids

        DETACH DELETE r
        """
    ).consume()
    
    qd.nodes_deleted += result.counters.nodes_deleted
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after
    
    Console.log(Level.DEBUG, "Creating or updating routes")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/routes.txt' AS route
        WITH route

        MATCH (r:Route {route_id: toString(route.route_id)})
        SET r.route_name = toString(route.route_short_name)

        WITH route, r

        MATCH (a:Agency {agency_id: route.agency_id})
        MERGE (a)-[:OPERATES]->(r)
        """
    ).consume()
    
    qd.labels_added = result.counters.labels_added
    qd.properties_set = result.counters.properties_set
    qd.nodes_created = result.counters.nodes_created
    qd.relationships_created = result.counters.relationships_created
    qd.execution_time += result.result_available_after

    return qd

def load_trips(session) -> QueryData:
    qd = QueryData()
    
    Console.log(Level.DEBUG, "Removing unused trips")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/trips.txt' AS trip
        WITH collect(DISTINCT trip.trip_id) AS trip_ids

        MATCH (t:Trip)
        WHERE NOT t.trip_id IN trip_ids

        DETACH DELETE t
        """
    ).consume()
    
    qd.nodes_deleted += result.counters.nodes_deleted
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after

    Console.log(Level.DEBUG, "Creating or updating trips")
    trips_file = f"{CSV_DIR}/bus_data/trips.txt"
    total_lines = sum(1 for _ in open(trips_file))
    step = 4000
    processed_lines = 0
    try:
        if total_lines > 0:
            Console.log(Level.TRACE, total_lines, f" results found in {trips_file}")
            while processed_lines < total_lines:

                result = execute_query(session, 
                    """
                    LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/trips.txt' AS trip
                    SKIP $processed_lines LIMIT $step
                    WITH trip

                    MERGE (t:Trip {trip_id: toString(trip.trip_id)})
                    SET t.headsign = toString(trip.trip_headsign),
                        t.accessible = toBoolean(toString(trip.wheelchair_accessible))

                    WITH trip, t

                    MATCH (r:Route {route_id: trip.route_id})
                    MERGE (t)-[:BELONGS_TO]->(r)

                    WITH trip, t

                    MATCH (c:Calendar {service_id: trip.service_id})
                    MERGE (t)-[:RUNS_ON]->(c)
                    """, processed_lines=processed_lines, step=step
                ).consume()
                
                qd.labels_added += result.counters.labels_added
                qd.properties_set += result.counters.properties_set
                qd.nodes_created += result.counters.nodes_created
                qd.relationships_created += result.counters.relationships_created
                qd.execution_time += result.result_available_after

                processed_lines += step
                Console.bar(min(processed_lines, total_lines), total_lines)

            if Console.cr == True: print()

        else:
            Console.log(Level.WARN, f"No results found in {trips_file}")

    except KeyboardInterrupt as e:
        if Console.cr == True: print()
        Console.log(Level.DEBUG, qd)
        Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")
        exit(1)
    
    return qd


def load_stops(session) -> QueryData:
    qd = QueryData()

    Console.log(Level.DEBUG, "Removing unused stops")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stops.txt' AS stop
        WITH collect(DISTINCT stop.stop_id) AS stop_ids

        MATCH (s:Stop)
        WHERE NOT s.stop_id IN stop_ids
          AND NOT (s)<-[:CHILD_OF]-(:Stop)

        DETACH DELETE s
        """
    ).consume()
    
    qd.nodes_deleted += result.counters.nodes_deleted
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after
    
    Console.log(Level.DEBUG, "Removing obsolete parent stops")
    result = execute_query(session, 
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

        DETACH DELETE p
        """
    ).consume()
    
    qd.nodes_deleted += result.counters.nodes_deleted
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after

    Console.log(Level.DEBUG, "Creating or updating stops")
    stops_file = f"{CSV_DIR}/bus_data/stops.txt"
    total_lines = sum(1 for _ in open(stops_file))
    step = 10000
    processed_lines = 0
    try:
        if total_lines > 0:
            Console.log(Level.TRACE, total_lines, f" results found in {stops_file}")
            while processed_lines < total_lines:

                result = execute_query(session, 
                    """
                    LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stops.txt' AS stop
                    SKIP $processed_lines LIMIT $step
                    WITH stop

                    WHERE stop.location_type IS NULL OR toInteger(stop.location_type) = 0

                    MERGE(s:Stop {stop_id: toString(stop.stop_id)})
                    SET s.stop_name = toString(stop.stop_name),
                        s.accessible = toBoolean(toInteger(stop.wheelchair_boarding)),
                        s.location = point({
                            latitude: toFloat(stop.stop_lat), 
                            longitude: toFloat(stop.stop_lon)
                        })
                    """, processed_lines=processed_lines, step=step
                ).consume()
                
                qd.labels_added += result.counters.labels_added
                qd.properties_set += result.counters.properties_set
                qd.nodes_created += result.counters.nodes_created
                qd.relationships_created += result.counters.relationships_created
                qd.execution_time += result.result_available_after

                processed_lines += step
                Console.bar(min(processed_lines, total_lines), total_lines)

            if Console.cr == True: Console.log()

        else:
            Console.log(Level.WARN, f"No results found in {stops_file}")
            
    except KeyboardInterrupt as e:
        if Console.cr == True: Console.log()
        Console.log(Level.DEBUG, qd)
        Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")
        exit(1)

    Console.log(Level.DEBUG, "Creating or updating parent stops")
    result = execute_query(session, 
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
        """
    ).consume()
    
    qd.labels_added += result.counters.labels_added
    qd.properties_set += result.counters.properties_set
    qd.nodes_created += result.counters.nodes_created
    qd.relationships_created += result.counters.relationships_created
    qd.execution_time += result.result_available_after

    return qd

def generate_parent_stops(session) -> QueryData:
    Console.log(Level.DEBUG, "Generating parents for nearby stops...")
    qd = QueryData()

    Console.log(Level.DEBUG, "Grouping similar stops")
    cgs1, ggs = execute_query(session,
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

        WITH s1 {stop_name:s1.stop_name, stop_id:s1.stop_id}, collect(s2 {stop_name:s2.stop_name, stop_id:s2.stop_id}) AS s2s
        WITH s1 AS fs, ([s1] + s2s) AS gs

        UNWIND gs AS s
        ORDER BY s.stop_id
        WITH fs, collect(s) AS gs
        WITH DISTINCT gs
        WITH gs[0] AS fs, gs

        RETURN count(gs) AS cgs1, collect(gs) AS ggs
        """
    ).single()

    if cgs1 == 0:
        Console.log(Level.DEBUG, "No stops could be grouped")
    elif cgs1 > 0:
        Console.log(Level.DEBUG, "Daisy chaining groups...")

        chain_count=1
        while True:
            cgs2, ggs = execute_query(session,
                """
                UNWIND $ggs AS gs
                UNWIND gs AS s
                WITH s, collect(gs) AS gs
                WITH DISTINCT REDUCE(res = [], g IN gs | apoc.coll.union(res, g)) AS gs
                RETURN count(gs) AS cgs2, collect(gs) AS ggs
                """,
                ggs=ggs
            ).single()

            Console.log(Level.TRACE, cgs1, ', ', cgs2)

            if cgs1 == cgs2:
                break
            else:
                cgs1 = cgs2
                chain_count += 1

        Console.log(Level.DEBUG, "Generating parent stops...")
        result = execute_query(session,
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
            ON CREATE SET
                p.stop_name = toString(stop_name),
                p.accessible = gs[0].accessible,
                p.location = point({
                    latitude: toFloat(lat), 
                    longitude: toFloat(lng)
                })
            WITH p, gs
            UNWIND gs AS g
            MATCH (s:Stop {stop_id: g.stop_id})
            MERGE (p)<-[:CHILD_OF]-(s)
            """,
            ggs=ggs
        ).consume()
        
        qd.nodes_created += result.counters.nodes_created
        qd.relationships_created += result.counters.relationships_created
        qd.properties_set += result.counters.properties_set
        qd.execution_time += result.result_available_after

    return qd
        
def link_nearby_stops(session) -> QueryData:
    Console.log(Level.DEBUG, "Linking stops within 1km of each other")
    qd = QueryData()

    Console.log(Level.DEBUG, "Removing relationships for stops that are more then 1000m apart")
    result = execute_query(session, 
        """
        MATCH (s1:Stop)-[n:NEARBY]->(s2:Stop) 
        WITH s1.location AS sl1, n, s2.location AS sl2
        WHERE point.distance(sl1, sl2) > 1000 
        DELETE n
        """
    ).consume()
    
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after
    
    Console.log(Level.DEBUG, "Creating relationships for stops that are less then 1000m apart")
    result = execute_query(session, 
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
        MERGE (s1)-[:NEARBY {distWalked: dist}]-(s2)
        """
    ).consume()
    
    qd.labels_added += result.counters.labels_added
    qd.properties_set += result.counters.properties_set
    qd.relationships_created += result.counters.relationships_created
    qd.execution_time += result.result_available_after
    
    return qd

def load_stoptimes(session) -> QueryData:
    qd = QueryData()

    Console.log(Level.DEBUG, "Removing unused stop times")
    Console.log(Level.TRACE, 
        "Getting stop times that have any of the following: \n"
        "-- no trip assigned \n",
        "-- no stops are assigned\n"
        "-- an invalid trip_id assigned \n",
        "-- an invalid stop_sequence assigned\n"
    )
    result = execute_query(session, 
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

        RETURN collect(elementId(st)) AS stids, count(st) as stc
        """
    )

    stids, stc = result.single()
    result = result.consume()

    qd.execution_time = result.result_available_after

    total_lines = stc
    step = 100000
    processed_lines = 0

    try:
        if total_lines > 0:
            Console.log(Level.TRACE, total_lines, f" deleting {total_lines} stop times")
            Console.bar(min(processed_lines, total_lines), total_lines)
            while processed_lines < total_lines:
                result = execute_query(session,
                    """
                    WITH $stids AS stids
                    UNWIND stids AS stid
                    SKIP $processed_lines LIMIT $step
                    MATCH (st:StopTime)
                    WHERE elementId(st) = stid

                    DETACH DELETE st
                    """, stids=stids, processed_lines=processed_lines, step=step
                ).consume()
                
                qd.nodes_deleted += result.counters.nodes_deleted
                qd.relationships_deleted += result.counters.relationships_deleted
                qd.execution_time += result.result_available_after

                processed_lines += step
                Console.bar(min(processed_lines, total_lines), total_lines)

            if Console.cr == True: Console.log()

        else:
            Console.log(Level.TRACE, f"No stoptimes to be deleted")

    except KeyboardInterrupt as e:
        if Console.cr == True: Console.log()
        Console.log(Level.DEBUG, qd)
        Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")
        exit(1)
    
    Console.log(Level.DEBUG, "Creating or updating stop times")
    stoptimes_file = f"{CSV_DIR}/bus_data/stop_times.txt"
    total_lines = sum(1 for _ in open(stoptimes_file))
    step = 100000
    processed_lines = 0
    try:
        if total_lines > 0:
            Console.log(Level.TRACE, f"{total_lines} stop times found")
            Console.bar(min(processed_lines, total_lines), total_lines)
            while processed_lines < total_lines:
                result = execute_query(session, 
                    """
                    LOAD CSV WITH HEADERS FROM 'file:///recce/bus_data/stop_times.txt' AS stoptime
                    SKIP $processed_lines LIMIT $step
                    WITH stoptime

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
                    """, processed_lines=processed_lines, step=step
                ).consume()

                qd.labels_added += result.counters.labels_added
                qd.properties_set += result.counters.properties_set
                qd.nodes_created += result.counters.nodes_created
                qd.relationships_created += result.counters.relationships_created
                qd.execution_time += result.result_available_after

                processed_lines += step
                Console.bar(min(processed_lines, total_lines), total_lines)

            if Console.cr == True: Console.log()
        else:
            Console.log(Level.WARN, f"No results found in {stoptimes_file}")

    except KeyboardInterrupt as e:
        if Console.cr == True: Console.log()
        Console.log(Level.DEBUG, qd)
        Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")
        exit(1)

    Console.log(Level.DEBUG, "Linking all stop times that precede one another")
    stoptimec = execute_query(session,"MATCH (n:StopTime)-[:PART_OF]->(t:Trip) RETURN count(n) AS c").single()[0]
    total_lines = stoptimec
    step = 50000
    processed_lines = 0
    try:
        if total_lines > 0:
            Console.log(Level.TRACE, f"{total_lines} stop times found")
            while processed_lines < total_lines:
                Console.bar(min(processed_lines, total_lines), total_lines)

                result = execute_query(session, 
                    """
                    MATCH (st:StopTime)-[:PART_OF]->(t:Trip)
                    ORDER BY t.trip_id, st.seq
                    WITH t.trip_id AS trip_id, collect(st) AS sts
                    UNWIND range(0, size(sts) - 1) AS i
                    WITH trip_id, sts[i] AS st1, sts[i+1] AS st2
                    WHERE st2 IS NOT NULL
                    WITH st1, st2
                    SKIP $processed_lines LIMIT $step

                    MERGE (st1)-[pre:PRECEDES]->(st2)
                    SET pre.timeTraveled = st2.departure_time - st1.arrival_time
                    """, processed_lines=processed_lines, step=step
                ).consume()
                
                qd.labels_added += result.counters.labels_added
                qd.properties_set += result.counters.properties_set
                qd.nodes_created += result.counters.nodes_created
                qd.relationships_created += result.counters.relationships_created
                qd.execution_time += result.result_available_after

                processed_lines += step
                Console.bar(min(processed_lines, total_lines), total_lines)
                
            if Console.cr == True: Console.log()
                
        else:
            Console.log(Level.WARN, f"No stoptimes found")

    except KeyboardInterrupt as e:
        if Console.cr == True: Console.log()
        Console.log(Level.DEBUG, qd)
        Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")
        exit(1)
        
    return qd


def load_venues(session) -> QueryData:
    qd = QueryData()

    Console.log(Level.DEBUG, "Removing unused venues")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/venues.txt' AS venue
        WITH collect(venue.id) AS venue_ids

        MATCH (v:Venue)
        WHERE NOT v.venue_id IN venue_ids

        DETACH DELETE v
        """
    ).consume()
    
    qd.nodes_deleted += result.counters.nodes_deleted
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after

    Console.log(Level.DEBUG, "Removing relationships for venues more then 1km from stop")
    result = execute_query(session, 
        """
        MATCH (v:Venue)<-[h:HAS]-(s:Stop)
        WHERE point.distance(v.location, s.location) > 1000

        DELETE h
        """
    ).consume()
    
    qd.relationships_deleted += result.counters.relationships_deleted
    qd.execution_time += result.result_available_after

    Console.log(Level.DEBUG, "Creating or updating venues")
    result = execute_query(session, 
        """
        LOAD CSV WITH HEADERS FROM 'file:///recce/osm_data/venues.txt' AS venue
        WITH collect(venue.id) AS venue_ids

        MATCH (v:Venue)
        WHERE NOT v.venue_id IN venue_ids

        DETACH DELETE v
        """
    ).consume()
    
    qd.labels_added += result.counters.labels_added
    qd.properties_set += result.counters.properties_set
    qd.nodes_deleted += result.counters.nodes_created
    qd.execution_time += result.result_available_after

    Console.log(Level.DEBUG, "Linking stops and venues within 1km")
    result = execute_query(session, 
        """
        MATCH (s:Stop)
        WHERE (NOT (s)-[:CHILD_OF]-(:Stop) OR (:Stop)-[:CHILD_OF]->(s))
        WITH s

        MATCH (v:Venue)
        WHERE point.distance(s.location, v.location) < 1000
        WITH v, s, point.distance(s.location, v.location) AS dist

        MERGE (s)-[h:HAS]->(v)
        SET h.distWalked = dist
        """
    ).consume()
    
    qd.labels_added += result.counters.labels_added
    qd.properties_set += result.counters.properties_set
    qd.nodes_deleted += result.counters.nodes_created
    qd.execution_time += result.result_available_after

    return qd


def main():
    try:

        Console.log(Level.INFO, "Downloading required data...")
        download_files()

        Console.log()
        Console.log(Level.INFO, "Importing data into neo4j...")
        driver, session = authenticate_neo4j()

        Console.log()
        Console.log(Level.INFO, "Ensuring neo4j constraints and indexes...")
        qd = QueryData()
        qd += create_constraint(session, "Agency", "agency_id")
        qd += create_constraint(session, "Route", "route_id")
        qd += create_constraint(session, "Calendar", "service_id")
        qd += create_constraint(session, "Stop", "stop_id")
        qd += create_constraint(session, "Venue", "venue_id")
        if qd > 0: Console.log(Level.DEBUG, qd)
        else: Console.log(Level.DEBUG, f"No constraints were created after {qd.execution_time}ms")

        qd = QueryData()
        qd += create_index(session, "PRECEDES", "timeTraveled", "RANGE", True)
        qd += create_index(session, "HAS", "distWalked", "RANGE", True)
        qd += create_index(session, "Venue", "location", "POINT")
        qd += create_index(session, "Venue", "category", "RANGE")
        qd += create_index(session, "Stop", "location", "POINT")
        qd += create_index(session, "Stop", "stop_name", "TEXT")
        qd += create_index(session, "Route", "route_name", "TEXT")
        qd += create_index(session, "StopTime", "seq", "RANGE")
        qd += create_index(session, "StopTime", "arrival_time", "RANGE")
        qd += create_index(session, "StopTime", "departure_time", "RANGE")
        qd += create_index(session, "Calendar", "mon", "RANGE")
        qd += create_index(session, "Calendar", "tue", "RANGE")
        qd += create_index(session, "Calendar", "wed", "RANGE")
        qd += create_index(session, "Calendar", "thu", "RANGE")
        qd += create_index(session, "Calendar", "fri", "RANGE")
        qd += create_index(session, "Calendar", "sat", "RANGE")
        qd += create_index(session, "Calendar", "sun", "RANGE")
        if qd > 0: Console.log(Level.DEBUG, qd)
        else: Console.log(Level.DEBUG, f"No indexes were created after {qd.execution_time}ms")


        Console.log()
        Console.log(Level.INFO, "Loading and updating service dates and trips...")
        qd = load_calendars(session)
        qd.label_name = "(:Calendar)"
        if qd > 0: Console.log(Level.DEBUG, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")

        qd = load_agencies(session)
        qd.label_name = "(:Agency)"
        if qd > 0: Console.log(Level.DEBUG, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")

        qd = load_routes(session)
        qd.label_name = "(:Route)"
        if qd > 0: Console.log(Level.DEBUG, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")

        qd = load_trips(session)
        qd.label_name = "(:Trip)"
        if qd > 0: Console.log(Level.DEBUG, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")


        Console.log()
        Console.log(Level.INFO, "Loading and updating stops...")
        qd = load_stops(session)
        qd.label_name = "(:Stop)"
        if qd > 0: Console.log(Level.DEBUG, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")

        qd = generate_parent_stops(session)
        qd.label_name = "(:Stop)"
        if qd > 0: Console.log(Level.INFO, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")

        qd = link_nearby_stops(session)
        qd.label_name = "[:NEARBY]"
        if qd > 0: Console.log(Level.INFO, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")


        Console.log()
        Console.log(Level.INFO, "Loading and updating stop times...")
        qd = load_stoptimes(session)
        qd.label_name = "(:StopTime)"
        if qd > 0: Console.log(Level.INFO, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")

        Console.log()
        Console.log(Level.INFO, "Loading and updating venues...")
        qd = load_venues(session)
        qd.label_name = "(:Venue)"
        if qd > 0: Console.log(Level.INFO, qd)
        else: Console.log(Level.DEBUG, f"No changes were made to \033[1m{qd.label_name}\033[0m after {qd.execution_time}ms")

    except KeyboardInterrupt as e:
        if Console.cr == True: Console.log()
        Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")

    except Exception as e:
        if Console.cr == True: Console.log()
        Console.log(Level.FATAL, type(e).__name__, ": ", e)

    finally:
        try:
            driver.close()
        except Exception as e:
            if Console.cr == True: Console.log()
            Console.log(Level.WARN, "Driver failed to close")
        
        exit(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="ETL script for extracting bus and osm data and loading it into a Neo4j database")
    parser.add_argument("-l", "--level", help="The minimum level required for a message to be logged", type=str, default="DEBUG")
    
    parse_level = parser.parse_args().level

    try:
        if parse_level.isdigit() and int(parse_level) < 3:
            MIN_LOG_LVL = int(parser.parse_args().level)
        else:
            MIN_LOG_LVL = Level[parse_level.upper()].value
    except KeyError:
        parser.error(f"Invalid level argument: {parse_level}. Must be (0, 1, 2) or [TRACE, DEBUG, INFO]")

    init_directory(f"{TMP_DIR}/log")
    file = open(f"{TMP_DIR}/log/trace.txt", 'w')

    main()
