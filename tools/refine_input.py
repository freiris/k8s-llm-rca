#!/usr/bin/env python
'''
refine each error_message with namespace, timestamp and uuid
'''

import os
import neo4j
import csv
import argparse
from neo4j import GraphDatabase

from common.neo4j_query_executor import Neo4jQueryExecutor


def make_query(error_message, namespace, timestamp=None):
    query_parts = []
    query_parts.append(f""" 
    MATCH (n1:Event)-[s1:HasEvent]->(N1:EVENT)
    WHERE n1.namespace2 = {repr(namespace)} 
    AND N1.message CONTAINS {repr(error_message)}
    """)
    
    if timestamp != None:
        query_parts.append(f"""
    AND N1.timestamp = {repr(timestamp)}
    """)
    
    query_parts.append(f"""
    RETURN n1.namespace2 AS namespace, N1.message AS error_message,\
            N1.timestamp AS timestamp, n1.uid2 AS uuid
    LIMIT 1
    """)
    
    complete_query = '\n'.join(query_parts) 
    
    return complete_query

def run(input_filename, output_filename):
    print(f"Input file: {input_filename}")
    print(f"Output file: {output_filename}")
    print('+' * 120 + '\n')

    print("create executor and init connection")
    # Create an instance of the executor class
    stategraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.174:7687", "neo4j", "yong")
  
    # read from old csv file
    rows = []
    with open(input_filename, newline='') as csv_in:
        csvreader = csv.reader(csv_in)
        # Skip the header
        next(csvreader)
        for row in csvreader:
            rows.append(row)
    
    print('+' * 120 + '\n')

    # add uuid and timestamp (if not exist) for each row
    new_rows = []
    for row in rows:
        print(row)

        namespace = row[0]
        error_message = row[1]
        if len(row) > 2:
            timestamp = row[2]
        else:
            timestamp = None
        
        cypher_query = make_query(error_message, namespace, timestamp)
        records = stategraph_query_executor.run_query(cypher_query)
        new_rows.append(records[0])
   
    # write to new csv file
    os.makedirs(os.path.dirname(output_filename), exist_ok=True)
    with open(output_filename, 'w') as csv_out:
        csvwriter = csv.writer(csv_out)
        fields = ['namespace', 'error_message', 'timestamp', 'uuid']
        csvwriter.writerow(fields)
        csvwriter.writerows(new_rows)


    print("close connection")
    # Close the connection when done
    stategraph_query_executor.close()


if __name__ == "__main__":
    # Initialize parser
    parser = argparse.ArgumentParser(
        description="Process input and output files."
    )

    # Add arguments for input and output file paths
    parser.add_argument(
        '-i', '--input-file',
        type=str,
        required=True,
        help='Path to the input file'
    )
    parser.add_argument(
        '-o', '--output-file',
        type=str,
        required=True,
        help='Path to the output file'
    )

    # Parse arguments
    args = parser.parse_args()

    # Pass the command line arguments to main function
    run(args.input_file, args.output_file)


