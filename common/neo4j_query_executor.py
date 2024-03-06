#!/usr/bin/env python

from neo4j import GraphDatabase

# Define a class for interacting with Neo4j
class Neo4jQueryExecutor:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.driver.verify_connectivity()

    def close(self):
        # Close the connection to the database
        self.driver.close()

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            # Run the query and immediately consume the results
            result = session.run(query, parameters)

            # Convert the result to a list so we can iterate multiple times if needed
            records = list(result)

            # You can now return the list of records, process it here, or both
            return records

