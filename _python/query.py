
from logger import Console, Level
from neo4j import GraphDatabase, Result, ResultSummary, NotificationMinimumSeverity 
from getpass import getuser
import re
import os

class QueryData:
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

    def __init__(self, other = None):
        if other and isinstance(other, ResultSummary):
            for attr in self.__annotations__:
                if attr == 'execution_time':
                    setattr(self, attr, getattr(self, attr) + other.result_available_after)
                else:
                    setattr(self, attr, getattr(self, attr) + getattr(other.counters, attr))

    def __iadd__(self, other):
        for attr in self.__annotations__:
            if isinstance(other, ResultSummary):
                if attr == 'execution_time':
                    setattr(self, attr, getattr(self, attr) + other.result_available_after)
                else:
                    setattr(self, attr, getattr(self, attr) + getattr(other.counters, attr))
                
            elif isinstance(other, QueryData):
                setattr(self, attr, getattr(self, attr) + getattr(other, attr))
            
        return self
    
    def __gt__(self, other):
        if isinstance(other, int):
            return any(getattr(self, attr) > other for attr in self.__annotations__)
        
    def __str__(self):
        return '\n'.join([
            f"{getattr(self, attr)} {attr}" 
            for attr in self.__annotations__ if getattr(self, attr) > 0 or attr == 'execution_time'
        ])

class Database:
    _NEO4J_URL = "bolt+s://neo4j.aneur.info"
    _driver = None
    _session = None

    _username = ''
    _password = ''
    _database = ''

    def auth():
        attempts = 0
        while True:
            Database._database = Console.inp("Enter Neo4j Database", False, "neo4j")
            Database._username = Console.inp("Enter Neo4j Username", False, "neo4j")
            Database._password = Console.inp("Enter Neo4j Password", True)

            Database._driver = GraphDatabase.driver(
                Database._NEO4J_URL, 
                auth=(Database._username, Database._password),
                notifications_min_severity='OFF'
            )

            try:
                Database._session = Database._driver.session(database=Database._database)
                Database._session.run("RETURN 1")
                Console.log(Level.TRACE, "Authenticated successfully")
                return True

            except Exception as e:
                Console.log(Level.ERROR, "Authentication failed. Please try again")
                Console.log(Level.TRACE, e.message)
                attempts += 1
                if attempts > 4:
                    Console.log(Level.FATAL, "Failed too many times. Exiting the script.")
                    os._exit(1)

    def _exec(query: str, **kwargs) -> Result:
        result = Database._session.run(query, kwargs)
        Console.log(Level.TRACE, "Executed query: \n", re.sub(r"^( +)?", '>  ', query, flags=re.MULTILINE))
        return result

    def exec(query: str, **kwargs) -> QueryData:
        if Database._driver is None: Database.auth()

        result = Database._exec(query, **kwargs)

        qd = QueryData(result.consume())
        Console.log(Level.TRACE, qd)
        return qd

    def exec_loop(query: str, action: str, step: int = 1000, total: int = 0, **kwargs) -> QueryData:
        if Database._driver is None: Database.auth()
        qd = QueryData()
        
        try: 
            if total == 0:
                count_query = query + '\n' + "RETURN count(*) AS c"

                Console.start_cycle(Level.DEBUG, "Processing total batch size")
                response = Database._exec(count_query, **kwargs)
                total = response.single()[0]
                qd += response.consume()
                Console.end_cycle()

            if total > 0:
                Console.log(Level.TRACE, f"Iterating through {total} results")
                i = 0
                Console.bar(min(i, total), total)
                while i < total:

                    q = query + "\n"
                    q += f"SKIP {i} LIMIT {step} \n"
                    q += action
                    response = Database._exec(q, **kwargs)

                    qd += response.consume()
                    i += step
                    
                    Console.log(Level.TRACE, qd)
                    Console.bar(min(i, total), total)
                if Console.cr: Console.log()

            else:
                Console.log(Level.TRACE, "No results to iterate through")

        except KeyboardInterrupt:
            if Console.cr: Console.log()
            Console.log(Level.TRACE, qd)
            Console.log(Level.FATAL, "Keyboard interrupt, exiting application...")
            os._exit(1)
            
        return qd
