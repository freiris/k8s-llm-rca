#!/usr/bin/env python

import os
import openai
import time
import json
import neo4j
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

    errorMessages = [ """Error creating: pods "es-white-list-cronjob-1607752440-gprx7" is forbidden: exceeded quota: compute-resources-dumeng1, requested: pods=1, used: pods=50, limited: pods=50""",
                     """MountVolume.SetUp failed for volume "pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3" : mount failed: exit status 32 Mounting command: systemd-run Mounting arguments: --description=Kubernetes transient mount for /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 --scope -- mount -t nfs 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 Output: Running scope as unit: run-re511f81c07574a6a84df041848b3347f.scope mount.nfs: mounting 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 failed, reason given by server: No such file or directory""",
                     """MountVolume.SetUp failed for volume "es-account-token-k29vm" : secret "es-account-token-k29vm" not found""",
                     """MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found""",
                     """Failed create pod sandbox: rpc error: code = Unknown desc = failed to set up sandbox container "9a71227eb35345ead771b9f20ce90e2402641784c1866710c64adaaf0fbac1f2" network for pod "es-cronjob-1607813100-dqqg6": networkPlugin cni failed to set up pod "es-cronjob-1607813100-dqqg6_fanxy1" network: failed to Statfs "/proc/12631/ns/net": no such file or directory""",
                     """pod has unbound immediate PersistentVolumeClaims""",
                     """MountVolume.SetUp failed for volume "pvc-6d127f0b-216d-4bb0-a967-6620c1671be6" : stat /var/lib/kubelet/pods/131523d6-21b1-4d85-bfef-da4fbde98991/volumes/kubernetes.io~nfs/pvc-6d127f0b-216d-4bb0-a967-6620c1671be6: stale NFS file handle""",
                    """Unable to attach or mount volumes: unmounted volumes=[example-pv-storage], unattached volumes=[example-pv-storage default-token-l4rcp]: error processing PVC lizhiliang1/example-pvc1: PVC is being deleted""",
                     """create Pod yanghao71-c2-0 in StatefulSet yanghao71-c2 failed error: pods "yanghao71-c2-0" is forbidden: exceeded quota: compute-resources-yanghao71, requested: limits.memory=60Gi, used: limits.memory=1778Gi, limited: limits.memory=1800Gi""",
                     """create Pod yanghao71-c2-0 in StatefulSet yanghao71-c2 failed error: pods "yanghao71-c2-0" is forbidden: exceeded quota: compute-resources-yanghao71, requested: pods=1, used: pods=50, limited: pods=50"""

            ]
    start_time = time.time()

    for errorMessage in errorMessages[1:2]:
        print(errorMessage)
        #message = input("Please input the message: \n")

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


        # find metapaths in metagraph from srcKind to destKind, not include the EVENT and Event
        print('test find_metapath()')
        destKind = destRelevant['DestinationKind']
        relevantResources = destRelevant['RelevantResources']
        #intermediateKinds = [x for x in relevantResources if x not in [srcKind, destKind]]
        intermediateKinds = [x for x in relevantResources if (x not in [srcKind, destKind]) and (x in nativeKinds or x in externalKinds)]
        
        # 
        metapaths = find_metapath(metagraph_query_executor, srcKind, destKind, intermediateKinds)
        for metapath in metapaths:
            # generate cypher query based on the extended metapath string (with EVENT and Event)
            extend_metapath = extend_metapath_construct_string(metapath)
           
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

            # if gpt4 can not generate syntax-correct query, 
            # or the result of the query is empty (usually due to semantic error)
            # we will try it again with human_generate_cypher_query
            if (attempt == max_attempts-1) or (len(records) == 0):
                print('#' * 100)
                print(f'manually generate cypher query for the following extended metapath: \n {extend_metapath}') 
                cypher_query_2 = human_generate_cypher_query(extend_metapath, errorMessage) 
                records = run_and_filter_query(stategraph_query_executor, cypher_query_2)

            for record in records:
                report, path_clues = check_statepath(stategraph_query_executor, semanticAnalyzer, record)
                print(report)
                '''
                for k in path_clues.keys():
                    print('-' * 100)
                    print(path_clues[k][0])
                '''

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


