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
from generate_query.generate_query_extend import *
from check_state.analyze_root_cause import *


def get_srckind_destkind_metapaths(error_message, prompt_template, native_kinds, external_kinds,\
                        stategraph_query_executor, metagraph_query_executor,rootCauseLocator):
    # find srckind in stategraph according to message, (Event)-[involvedObject_uid]->(srckind)
    print('test find_srcKind()')
    srckind = find_srcKind(stategraph_query_executor, error_message)

    # find destkind and relevant_resources
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            print('$' * 100)
            print('test find destkind and relevant_resources')
            dest_relevant = find_destKind_relevantResources(error_message, srckind, prompt_template, rootCauseLocator)
            break
        except json.decoder.JSONDecodeError as e:
            print(f"JSON Error occurred: {str(e)}")
            print(dest_relevant)
            exception_message = f"The dest_relavant encounters the following exception:\
                    \nJSON Error occurred: {str(e)}\
                    \nmake sure to return the output in JSON format, and put it in ```json <dest_relevant> ```"
            rootCauseLocator.add_message(exception_message)
            continue
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            print(dest_relevant)
            exception_message = f"The dest_relevant encounters encounters the following exception:\
                    \nAn unexpected error occurred: {str(e)}\
                    \nBased on the exception details above, please generate a correct dest_relevant."
            rootCauseLocator.add_message(exception_message)
            continue

    destkind = dest_relevant['DestinationKind']
    relevant_resources = dest_relevant['RelevantResources']
    interkinds = [x for x in relevant_resources if (x not in [srckind, destkind])\
                                and (x in native_kinds or x in external_kinds)]
    print(f'srckind = {srckind}, destkind = {destkind}, interkinds = {interkinds}')

    # metapaths is not guaranteed to be found, for example, Pod->StorageClass
    # the proposed destkind is not reachable from srckind
    metapaths = find_metapath(metagraph_query_executor, srckind, destkind, interkinds)

    # locator_attmpts = attmpts+1
    return srckind, destkind, metapaths, attempt+1

     

def generate_query_and_get_record(metapath, error_message, namespace, timestamp, uuid,\
                                cypherQueryGenerator, stategraph_query_executor ):
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
            cypher_query = generate_cypher_query(extend_metapath, error_message, namespace, timestamp, uuid, cypherQueryGenerator)
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
        cypher_query_2 = human_generate_cypher_query(extend_metapath, error_message, namespace, timestamp, uuid)
        records = run_and_filter_query(stategraph_query_executor, cypher_query_2)

        analysis['human_cypher_query'] = cypher_query_2

    return records, analysis


# todo
def build_new_report(exception_message, semanticAnalyzer):
    semanticAnalyzer.add_message(exception_message)
    # run it
    print('run assistant')
    semanticAnalyzer.run_assistant()
    messages = semanticAnalyzer.wait_get_last_k_message(1)
    report = messages.data[0].content[0].text.value
    
    return report

def parse_report(report, semanticAnalyzer):
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            report_json = json.loads(report)
            break
        except json.decoder.JSONDecodeError as e:
            print(f"JSON Error occurred: {str(e)}")
            exception_message = f"The report encounters the following exception:\
                            \nJSON Error occurred: {str(e)}\
                            \nmake sure to return the output in JSON format,\
                            \nand must NOT contain any text outside the JSON structure."
            report = build_new_report(exception_message, semanticAnalyzer)
            continue
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            exception_message = f"The report encounters encounters the following exception:\
                            \nAn unexpected error occurred: {str(e)}\
                            \nBased on the exception details above, please generate a correct JSON-only report."
            report = build_new_report(exception_message, semanticAnalyzer)
            continue

    if (attempt == max_attempts-1):
        print('Can not parse the report into json format.')
        # todo
        print(report)
        time.sleep(10)
        return report

    return report_json


def investigate_statepath(records, stategraph_query_executor, semanticAnalyzer):
    analysis = dict()
    analysis['statepath'] = list()
    sp = dict()
    for record in records:
        report, path_clues = check_statepath(stategraph_query_executor, semanticAnalyzer, record)
        print(report)
        sp['report'] = parse_report(report, semanticAnalyzer)
        sp['clue'] = path_clues
        analysis['statepath'].append(sp)
    
    return analysis

def investigate_empty_statepath(destkind, error_message, semanticAnalyzer):
    # we double-checked with human-generated query, and confirm the non-existence of entity
    analysis = dict()
    print(f"Warning: There is not an Entity node for '{destkind}', which is an obvious error.")
    report, finding = build_report_for_empty_statepath(destkind, error_message, semanticAnalyzer)
    print(report)
    analysis['empty_statepath'] = list()
    empty_sp = dict()
    empty_sp['report'] = parse_report(report, semanticAnalyzer)
    empty_sp['clue'] = finding
    analysis['empty_statepath'].append(empty_sp)
   
    return analysis

'''
def get_total_token_usage(rootCauseLocator, cypherQueryGenerator, semanticAnalyzer, inner_start_time, inner_end_time):
    # we caculate the token cost for each message,
    # including rootCauseLocator, cypherQueryGenerator and semanticAnalyzer
    tmin = int(inner_start_time)
    tmax = int(inner_end_time)

    # at most 3 retries for each message
    token_usage_1 = rootCauseLocator.get_token_usage(tmin, tmax, 10)
    # at most 3 retries for each metapath, we find 5 metapaths at most now
    token_usage_2 = cypherQueryGenerator.get_token_usage(tmin, tmax, 40)
    # metapath from srckind to destkind has at most 3 edges, namely 4 nodes
    # therefore, at most 4 STATE nodes to check for each metapath
    token_usage_3 = semanticAnalyzer.get_token_usage(tmin, tmax, 60)

    token_usage = dict()
    token_usage['prompt_tokens'] = token_usage_1['prompt_tokens'] +\
                                    token_usage_2['prompt_tokens'] + token_usage_3['prompt_tokens']
    token_usage['completion_tokens'] = token_usage_1['completion_tokens'] +\
                                    token_usage_2['completion_tokens'] + token_usage_3['completion_tokens']
    token_usage['total_tokens'] = token_usage_1['total_tokens'] +\
                                    token_usage_2['total_tokens'] + token_usage_3['total_tokens']
    
    #result['token_usage_details'] = [token_usage_1, token_usage_2, token_usage_3]
    return token_usage
'''

def get_total_token_usage_new(rootCauseLocator, cypherQueryGenerator, semanticAnalyzer,\
        limit_rcl, limit_cqg, limit_sa, inner_start_time, inner_end_time):
    
    # we caculate the token cost for each message,
    # including rootCauseLocator, cypherQueryGenerator and semanticAnalyzer
    tmin = int(inner_start_time)
    tmax = int(inner_end_time)

    # at most 3 retries for each message
    token_usage_1 = rootCauseLocator.get_token_usage(tmin, tmax, limit_rcl)
    # at most 3 retries for each metapath, we find 5 metapaths at most now
    token_usage_2 = cypherQueryGenerator.get_token_usage(tmin, tmax, limit_cqg)
    # metapath from srckind to destkind has at most 3 edges, namely 4 nodes
    # therefore, at most 4 STATE nodes to check for each metapath
    token_usage_3 = semanticAnalyzer.get_token_usage(tmin, tmax, limit_sa)

    token_usage = dict()
    token_usage['prompt_tokens'] = token_usage_1['prompt_tokens'] +\
                                    token_usage_2['prompt_tokens'] + token_usage_3['prompt_tokens']
    token_usage['completion_tokens'] = token_usage_1['completion_tokens'] +\
                                    token_usage_2['completion_tokens'] + token_usage_3['completion_tokens']
    token_usage['total_tokens'] = token_usage_1['total_tokens'] +\
                                    token_usage_2['total_tokens'] + token_usage_3['total_tokens']
    
    #result['token_usage_details'] = [token_usage_1, token_usage_2, token_usage_3]
    return token_usage



def determine_retry(analysis):
    # there can be multiple metapaths for each error_message,
    # and each metapath can correspond to one/more statepath (or empty_statepath),
    # we only retry if ALL the metapaths and ALL statepaths/empty_statepaths can not explain the error_message
    # tips: empty_metapath does not have 'further_investigation', but we retry it by default
    for aly in analysis:
        if 'statepath' in aly:
            for x in aly['statepath']:
                if str(x['report']['further_investigation']).lower() == 'false':
                    return False
        elif 'empty_statepath' in aly:
            for x in aly['empty_statepath']:
                if str(x['report']['further_investigation']).lower() == 'false':
                    return False
    return True

# gpt-4 simplify the code
def determine_retry_simple(analysis):
    for aly in analysis:
        paths = aly.get('statepath', []) + aly.get('empty_statepath', []) # empty_metapath has not 'further_investigation' key
        if any(str(x['report']['further_investigation']).lower() == 'false' for x in paths):
            return False       
    return True

def refresh_context(rootCauseLocator):
    print('Refresh the context ...\n')
    rootCauseLocator.add_message("Let's ignore the previous predictions and refresh the context to make new independent prediction for next error message.")
    #rootCauseLocator.add_message(pre_defined_kinds_prompt)
    rootCauseLocator.run_assistant()
    messages = rootCauseLocator.wait_get_last_k_message(1)
    print(messages.data[0].content[0].text.value)
    print('^' * 100 + '\n')



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

    print('setup cypher_generator')
    cypherQueryGenerator = setup_cypher_generator()

    print('setup state_semantic_analyzer')
    semanticAnalyzer = setup_state_semantic_analyzer()
 
    # write to app.log for later check
    log_file = './log/app.log'
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    with open(log_file, 'a') as fo:
        fo.write(f"Input file: {input_file}\n")
        fo.write(f"Output file: {output_file}\n")
        #https_prefix = 'https://platform.openai.com/playground'
        # for gpt-4o
        #https_prefix = 'https://platform.openai.com/playground/assistants'
        https_prefix = 'https://oai.azure.com/portal/9be961fed7ab4b58b2e7bfd101fa56a3/assistants'
        
        fo.write(f'{https_prefix}?assistant={rootCauseLocator.assistant.id}&thread={rootCauseLocator.thread.id}\n')
        fo.write(f'{https_prefix}?assistant={cypherQueryGenerator.assistant.id}&thread={cypherQueryGenerator.thread.id}\n')
        fo.write(f'{https_prefix}?assistant={semanticAnalyzer.assistant.id}&thread={semanticAnalyzer.thread.id}\n')
        fo.write('-' * 100 + '\n')
    
   
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
    #refresh_counter = 1
    for row in rows[begin_index: end_index]:
        '''
        # refresh rootCauseLocator context for every k (i.e, 10) rows
        # to avoid bad prediction after a long time, which may stem from the long-memory-decay
        refresh_interval = 5 # or 10
        if refresh_counter % refresh_interval  == 0:
            refresh_context(rootCauseLocator)
        refresh_counter = (refresh_counter + 1) % refresh_interval
        '''

        # already proposed destkind, used for retry
        destkinds = list()
        max_attempt = 3
        for attempt in range(max_attempt):
            inner_start_time = time.time() 
            # get message counter of rootCauseLocator, cypherQueryGenerator, semanticAnalyzer
            # currently, we do not worry about overflow
            start_counter_rcl = rootCauseLocator.get_message_counter()
            start_counter_cqg = cypherQueryGenerator.get_message_counter()
            start_counter_sa = semanticAnalyzer.get_message_counter()

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
        
            # find destkind and metapaths for error_message
            srckind, destkind, metapaths, locator_attempts = get_srckind_destkind_metapaths(error_message, prompt_template,\
                        native_kinds, external_kinds, stategraph_query_executor, metagraph_query_executor,rootCauseLocator)

            result['srckind'] = srckind
            result['destkind'] = destkind
            result['locator_attempts'] = locator_attempts
            
            destkinds.append(destkind) 
            
            #'''
            result['analysis'] = list()
            visited_nodes_list = list()
            # we can not guarantee that metapaths will always exist, for example, Pod--->StorageClass has no path
            if metapaths is None:
                empty_metapath_analysis = {'empty_metapath': 
                                            [{'report': 
                                                {'conclusion': f'Can not find ANY metapath from {srckind} to {destkind}'}}]}
                result['analysis'].append(empty_metapath_analysis)
            else:
                for metapath in metapaths:
                    # todo: check the list to determine whether the entity-kind in metapath has been visited in statepath 
                    # if visited, we skip this metapath to avoid similar metapaths stem from multiple-edge
                    # i.e, Event->Job->Pod->ServiceAccount, both spec_serviceAccount and spec_serviceAccountName for the last edge
                    if metapath.nodes in visited_nodes_list:
                        continue

                    # generate cypher query for each metapath, and run the query in neo4j to retrieve records
                    records, analysis1 = generate_query_and_get_record(metapath, error_message, namespace, timestamp, uuid,\
                                cypherQueryGenerator, stategraph_query_executor )
            
                    # if no records found, there is an empty_statepath
                    if(len(records) == 0):
                        analysis2 = investigate_empty_statepath(destkind, error_message, semanticAnalyzer)
                    else:
                        # otherwise, investigate the entity+state in each statepath
                        analysis2 = investigate_statepath(records, stategraph_query_executor, semanticAnalyzer)
                        # todo: add the visited entity-kind to a list
                        visited_nodes_list.append(metapath.nodes)

                    # merge analysis1 and analysis2
                    analysis1.update(analysis2)
                    result['analysis'].append(analysis1)
            #'''
            
            # we only keep the time cost for each message, not for the metapaths
            inner_end_time = time.time()
            result['time_cost'] = inner_end_time - inner_start_time
            
            # get message counter of rootCauseLocator, cypherQueryGenerator, semanticAnalyzer at the end
            # currently, we do not worry about overflow
            end_counter_rcl = rootCauseLocator.get_message_counter()
            end_counter_cqg = cypherQueryGenerator.get_message_counter()
            end_counter_sa = semanticAnalyzer.get_message_counter()
            
            limit_rcl = end_counter_rcl - start_counter_rcl
            limit_cqg = end_counter_cqg - start_counter_cqg
            limit_sa = end_counter_sa - start_counter_sa

            # calculate the token usage 
            #result['token_usage'] = get_total_token_usage(rootCauseLocator, cypherQueryGenerator, semanticAnalyzer,\
            #                                    inner_start_time, inner_end_time)
            
            result['token_usage'] = get_total_token_usage_new(rootCauseLocator, cypherQueryGenerator, semanticAnalyzer,\
                                                            limit_rcl, limit_cqg, limit_sa,\
                                                            inner_start_time, inner_end_time)

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
            time.sleep(5)
            print('+' * 100 + '\n')
            
            # if current check can not explain the root cause, we require another new proposal
            # if True and (attempt < max_attempt-1):
            if determine_retry_simple(result['analysis']) and (attempt < max_attempt-1):
                # don't add retry_message for the last one, otherwise, it may affect the following error message
                add_retry_prompt(error_message, destkinds, rootCauseLocator)
                print('further investigation required, and prompt to retry')
                print('+' * 100 + '\n')
            else:
                print('Not need further investigate, current check can explain the root cause')
                print('+' * 100 + '\n')
                # we should explicitly refresh to get better prediction, the prompt-alone is not enough
                refresh_message = "Please ignore all previous predictions and make an independent prediction for the next error message based solely on its content."
                rootCauseLocator.add_message(refresh_message) 
                
                break


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


