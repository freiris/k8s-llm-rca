#!/usr/bin/env python

import os
import openai
import time
import json
import neo4j
import csv
import argparse
from itertools import islice
#from openai import OpenAI
from openai import AzureOpenAI
from neo4j import GraphDatabase

from common.neo4j_query_executor import Neo4jQueryExecutor
#from common.openai_generic_assistant import OpenAIGenericAssistant
from common.azure_openai_generic_assistant import OpenAIGenericAssistant

from find_metapath.find_srckind_destkind_metapath import *
#from generate_query.generate_query_extend import *
#from check_state.analyze_root_cause import *


# we move the try-exception into find_destKind_relevantResources()
def get_srckind_destkind_interkinds(error_message, prompt_template, native_kinds, external_kinds,\
                        stategraph_query_executor, metagraph_query_executor, rootCauseLocator):
    # find srckind in stategraph according to message, (Event)-[involvedObject_uid]->(srckind)
    print('test find_srcKind()')
    srckind = find_srcKind(stategraph_query_executor, error_message)

    # find destkind and relevant_resources
    dest_relevant, locator_attempts = find_destKind_relevantResources(error_message, srckind, prompt_template, rootCauseLocator)
    destkind = dest_relevant['DestinationKind']
    relevant_resources = dest_relevant['RelevantResources']
    interkinds = [x for x in relevant_resources if (x not in [srckind, destkind])\
                                and (x in native_kinds or x in external_kinds)]

    print(f'srckind = {srckind}, destkind = {destkind}, interkinds = {interkinds}')

    # metapaths is not guaranteed to be found, for example, Pod->StorageClass
    # the proposed destkind is not reachable from srckind
    metapaths = find_metapath(metagraph_query_executor, srckind, destkind, interkinds)

    return srckind, destkind, interkinds, locator_attempts

def run(input_file, output_file, begin_index, end_index):
    # show the input_file and output_file   
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    print('+' * 120 + '\n')
    
    # set up neo4j query executor and gpt assistant 
    print("create executor and init connection")
    metagraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.176:7687", "neo4j", "yong")
    stategraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.174:7687", "neo4j", "yong")

    print('create openai client with assistant and thread')
    print('setup root_cause_locator') 
    rootCauseLocator = setup_root_cause_locator()

    print('find native and external kinds and build prompt template')
    native_kinds, external_kinds = find_native_external_kinds(metagraph_query_executor)
    # we only pre_define the resource kinds once, to use fewer tokens 
    pre_defined_kinds_prompt = pre_defined_kinds(native_kinds, external_kinds)
    rootCauseLocator.add_message(pre_defined_kinds_prompt)

    prompt_template = build_prompt_template(native_kinds, external_kinds)
    
    # read lines from input_file
    rows = []
    with open(input_file, newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        # Skip the header
        next(csvreader)
        for row in csvreader:
            rows.append(row)

    for x in rows[begin_index: end_index]:
        print(x)
    
    print('+' * 100 + '\n')
   
    # total time cost for the code
    start_time = time.time()
    
    # for each error_message, we propose at most 3 check plans
    for row in rows[begin_index: end_index]:
        #max_attempt = 3
        max_attempt = 1 # change from 3 to 1
        for attempt in range(max_attempt):
            inner_start_time = time.time() 
            namespace = row[0]
            error_message = row[1]
            timestamp = row[2]
            uuid = row[3] if len(row) > 2 else None

            result = dict()
            result['error_message'] = error_message
            result['namespace'] = namespace
            result['timestamp'] = timestamp 
            result['uuid'] = uuid
        
            result['attempt'] = attempt+1
            
            print(error_message)
            srckind, destkind, interkinds, locator_attempts = get_srckind_destkind_interkinds(error_message, prompt_template,\
                        native_kinds, external_kinds, stategraph_query_executor, metagraph_query_executor,rootCauseLocator)
            result['srckind'] = srckind
            result['destkind'] = destkind
            result['interkinds'] = interkinds # add the interkinds
            result['locator_attempts'] = locator_attempts
            
            # we only keep the time cost for each message, not for the metapaths
            inner_end_time = time.time()
            result['time_cost'] = inner_end_time - inner_start_time
            
            # write the result for an error_message
            # if we use multiple-line json, we should seperate each record with comma (',')
            # and enclose all records with square brackets ('[]').for later pyspark processing.
            # or use single-line without comma and square brackets
            #os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'a') as json_file:
                json_record = json.dumps(result, indent=4)
                json_file.write(json_record + ',\n')

            print('+' * 100 + '\n')
            print(f'check the result in {output_file}')
            time.sleep(5) # keep sleep 5 sec to reduce the Request-Per-Minute to Azure
            print('+' * 100 + '\n')
            
    # total running time
    end_time = time.time()
    time_lapsed = end_time - start_time

    formated_start_time = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(start_time))
    formated_end_time = time.strftime('%Y/%m/%d %H:%M:%S', time.localtime(end_time))

    print('*' * 100)
    print(f"The code started at {formated_start_time}, ended at {formated_end_time}, and ran for {time_lapsed} seconds.")
    print('*' * 100)

    print("close connection")
    # Close the connection when done
    metagraph_query_executor.close()
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
    parser.add_argument(
        '-b', '--begin-index',
        type=int,
        #required=True,
        default=0,
        help='index to begin with in input file'
    )
    parser.add_argument(
        '-e', '--end-index',
        type=int,
        #required=True,
        default=None,
        help='index to end up in input file'
    )

    # Parse arguments
    args = parser.parse_args()

    # Pass the command line arguments to main function
    run(args.input_file, args.output_file, args.begin_index, args.end_index)


