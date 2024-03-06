#!/usr/bin/env python

import os
import openai
import time
import json
import neo4j
import csv
from itertools import islice
from openai import OpenAI
from neo4j import GraphDatabase

from common.neo4j_query_executor import Neo4jQueryExecutor
from common.openai_generic_assistant import OpenAIGenericAssistant

from find_metapath.find_srckind_metapath_neo4j import *
from generate_query.generate_query import *
from check_state.analyze_root_cause import *

def main():
    print("create executor and init connection")
    # Create an instance of the executor class
    metagraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.176:7687", "neo4j", "yong")
    stategraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.174:7687", "neo4j", "yong")

    print('create openai client with assistant and thread')
    print('setup root_cause_locator') 
    rootCauseLocator = setup_root_cause_locator()

    print('find native and external kinds and build prompt template')
    nativeKinds, externalKinds = find_native_external_kinds(metagraph_query_executor)
    promptTemplate = build_prompt_template(nativeKinds, externalKinds)

    print('setup cypher_generator')
    cypherQueryGenerator = setup_cypher_generator()

    print('setup state_semantic_analyzer')
    semanticAnalyzer = setup_state_semantic_analyzer()

    #time.sleep(300)
   
    input_filename = './data/mixed-example-10-3.csv'
    output_filename = './output/mixed-example-10-3c-result.json'

    errorMessages = []
    with open(input_filename, newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        # Skip the header
        next(csvreader)
        # Use islice to read first 10 lines of actual data
        #for row in islice(csvreader, 10):
        for row in csvreader:
            errorMessages.append(row[0])
    
    for x in errorMessages:
        print(x)
    
    print('+' * 150 + '\n')
    #time.sleep(300)
   
    # total time cost for the code
    start_time = time.time()

    for errorMessage in errorMessages[1:2]:
        inner_start_time = time.time() 

        result = dict()
        result['error_message'] = errorMessage

        print(errorMessage)

        # find srcKind in stategraph according to message, (Event)-[involvedObject_uid]->(srcKind)
        print('test find_srcKind()')
        srcKind = find_srcKind(stategraph_query_executor, errorMessage)

        # find destKind and relevantResources with gpt4 assistant
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                print('$' * 100)
                print('test find destKind and relevantResources')
                destRelevant = find_destKind_relevantResources(errorMessage, srcKind, promptTemplate, rootCauseLocator)
                break
            except json.decoder.JSONDecodeError as e:
                print(f"JSON Error occurred: {str(e)}")
                exception_message = f"The dest_relavant encounters the following exception:\
                    \nJSON Error occurred: {str(e)}\
                    \nmake sure to return the output in JSON format, and put it in ```json <dest_relevant> ```"
                rootCauseLocator.add_message(exception_message)
                continue
            except Exception as e:
                print(f"An unexpected error occurred: {str(e)}")
                exception_message = f"The dest_relevant encounters encounters the following exception:\
                    \nAn unexpected error occurred: {str(e)}\
                    \nBased on the exception details above, please generate a correct dest_relevant."
                rootCauseLocator.add_message(exception_message)
                continue

        result['locator_attempts'] = attempt + 1

        # find metapaths in metagraph from srcKind to destKind, not include the EVENT and Event
        print('test find_metapath()')
        destKind = destRelevant['DestinationKind']
        relevantResources = destRelevant['RelevantResources']
        intermediateKinds = [x for x in relevantResources if (x not in [srcKind, destKind])\
                                and (x in nativeKinds or x in externalKinds)]
        
        metapaths = find_metapath(metagraph_query_executor, srcKind, destKind, intermediateKinds)
        
        result['analysis'] = list()
        for metapath in metapaths:
            # generate cypher query based on the extended metapath string (with EVENT and Event)
            extend_metapath = extend_metapath_construct_string(metapath)
           
            analysis = dict()
            analysis['extend_metapath'] = extend_metapath

            max_attempts = 3
            for attempt in range(max_attempts): 
                try:
                    print('%' * 100)
                    print(f'attempt = {attempt}\n')
                    print(f'generate cypher query for the following extended metapath: \n {extend_metapath}')
                    cypher_query = generate_cypher_query(extend_metapath, errorMessage, cypherQueryGenerator)
                    records = run_and_filter_query(stategraph_query_executor, cypher_query)
                    # if succeed
                    break
                except neo4j.exceptions.CypherSyntaxError as e:
                    print(f"Cypher Syntax Error occurred: {str(e)}")
                    exception_message = f"The previous generated cypher query encounters the following exception:\
                    \nCypher Syntax Error occurred: {str(e)}\
                    \nBased on the exception details above, please generate a corrected version of the Cypher query."
                    cypherQueryGenerator.add_message(exception_message)
                    continue
                except Exception as e:
                    print(f"An unexpected error occurred: {str(e)}")
                    exception_message = f"The previous generated cypher query encounters the following exception:\
                    \nAn unexpected error occurred: {str(e)}\
                    \nBased on the exception details above, please generate a corrected version of the Cypher query."
                    cypherQueryGenerator.add_message(exception_message)
                    continue 
            # only add once 
            analysis['cypher_query'] = cypher_query   
            analysis['cypher_attempts'] = attempt + 1

            # if gpt4 can not generate syntax-correct query, 
            # or the result of the query is empty (usually due to semantic error)
            # we will try it again with human_generate_cypher_query
            if (attempt == max_attempts-1) or (len(records) == 0):
                print('#' * 100)
                print(f'manually generate cypher query for the following extended metapath: \n {extend_metapath}') 
                cypher_query_2 = human_generate_cypher_query(extend_metapath, errorMessage) 
                records = run_and_filter_query(stategraph_query_executor, cypher_query_2)
                
                analysis['human_cypher_query'] = cypher_query_2

            analysis['statepath'] = list()
            sp = dict()
            for record in records:
                report, path_clues = check_statepath(stategraph_query_executor, semanticAnalyzer, record)
                print(report)
                '''
                for k in path_clues.keys():
                    print('-' * 100)
                    print(path_clues[k][0])
                '''
                sp['report'] = report
                sp['clue'] = path_clues
                analysis['statepath'].append(sp)

            result['analysis'].append(analysis)

        # we only keep the time cost for each message, not for the metapaths
        inner_end_time = time.time()
        result['time_cost'] = inner_end_time - inner_start_time
        
        # we caculate the token cost for each message, 
        # including rootCauseLocator, cypherQueryGenerator and semanticAnalyzer 
        tmin = int(inner_start_time)
        tmax = int(inner_end_time)
        
        # at most 3 retries for each message 
        token_usage_1 = rootCauseLocator.get_token_usage(tmin, tmax, 10) 
        # at most 3 retries for each metapath, we find 2 metapaths at most now  
        token_usage_2 = cypherQueryGenerator.get_token_usage(tmin, tmax, 20) 
        # metapath from srckind to destkind has at most 3 edges, namely 4 nodes
        # therefore, at most 4 STATE nodes to check for each metapath
        token_usage_3 = semanticAnalyzer.get_token_usage(tmin, tmax, 30)

        token_usage = dict()
        token_usage['prompt_tokens'] = token_usage_1['prompt_tokens'] +\
                                    token_usage_2['prompt_tokens'] + token_usage_3['prompt_tokens']
        token_usage['completion_tokens'] = token_usage_1['completion_tokens'] +\
                                    token_usage_2['completion_tokens'] + token_usage_3['completion_tokens']
        token_usage['total_tokens'] = token_usage_1['total_tokens'] +\
                                    token_usage_2['total_tokens'] + token_usage_3['total_tokens']
        
        result['token_usage'] = token_usage
        #result['token_usage_details'] = [token_usage_1, token_usage_2, token_usage_3]

        # write the result for an error_message
        with open(output_filename, 'a') as json_file:
            json_record = json.dumps(result, indent=4)
            json_file.write(json_record + '\n')

        print('+' * 150)
        print(f'check the result in {output_filename}')
        time.sleep(10)
        print('+' * 150)

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
    main()


