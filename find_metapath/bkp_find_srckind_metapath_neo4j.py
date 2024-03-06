#!/usr/bin/env python

import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from neo4j_query_executor import Neo4jQueryExecutor
from openai_root_cause_locator import OpenAIRootCauseLocator
from openai_root_cause_locator import build_prompt_template

def find_native_external_kinds(query_executor):
    query = """
        MATCH (n1)
        WHERE n1.category IN ['NativeEntity', 'ExternalEntity']
        RETURN n1.category AS category, n1.kind AS kind
        """
    records = query_executor.run_query(query)
    nativeKinds = sorted([x['kind'] for x in records if (x['category'] == 'NativeEntity')])
    externalKinds = sorted([x['kind'] for x in records if (x['category'] == 'ExternalEntity')])
    return nativeKinds, externalKinds 


def find_srcKind(query_executor, message):
    query = """
        MATCH (n1:Event)-[s1:HasEvent]->(N1:EVENT)
        WHERE N1.message contains $message
        WITH n1, N1, s1
        MATCH (n1:Event)-[r1:ReferInternal]->(n2)
        WHERE r1.key = 'involvedObject_uid'
        RETURN distinct n2.kind2
        LIMIT 5;
        """
    parameters = {'message': message}
    # Run the query and process the results
    records = query_executor.run_query(query, parameters)
    srcKind = records[0]['n2.kind2']
    print('srcKind = %s' % srcKind)
    return srcKind


def find_metapath(query_executor, srcKind, destKind, intermediateKinds=None):
    # query with directed graph, support null intermeditateKinds
    query_directed = """
        MATCH path = (n1)-[*1..3]->(n2)
        WHERE n1.kind = $srcKind and n2.kind = $destKind
        AND all(node in nodes(path) WHERE single(x in nodes(path) WHERE x = node))
        AND all(node in nodes(path) WHERE not node.kind in ['Event', 'Namespace'])
        AND ($intermediateKinds IS NULL 
            OR size($intermediateKinds) = 0 
            OR any(node in nodes(path)[1..-1] WHERE node.kind in $intermediateKinds))
        RETURN path
        """

    # query with undirected graph, support null intermediateKinds
    query_undirected = """
        MATCH path = (n1)-[*1..3]-(n2)
        WHERE n1.kind = $srcKind and n2.kind = $destKind
        AND all(node in nodes(path) WHERE single(x in nodes(path) WHERE x = node))
        AND all(node in nodes(path) WHERE not node.kind in ['Event', 'Namespace'])
        AND ($intermediateKinds IS NULL 
            OR size($intermediateKinds) = 0 
            OR any(node in nodes(path)[1..-1] WHERE node.kind in $intermediateKinds))
        RETURN path
        """

    # src_dest are connected 
    query_single = """
        MATCH path = (n1)-[r1]-(n2)
        WHERE n1.kind = $srcKind and n2.kind = $destKind
        RETURN path
        """
    # srcKind-Namespace-destKind
    query_namespace = """
        MATCH path = (n1)-[r1]-(n2)-[r2]-(n3)
        WHERE n1.kind = $srcKind and n2.kind = 'Namespace' and n3.kind = $destKind
        RETURN path
        """


    # we prefer not use Namespace as intermediate kind, unless we have to include it 
    interKinds = [x for x in intermediateKinds if x != 'Namespace']

    #parameters = {'srcKind': 'Pod', 'destKind': 'nfs', 'intermediateKinds': ['PersistentVolumeClaim', 'PersistentVolume', 'Node']}
    parameters = {'srcKind': srcKind, 'destKind': destKind, 'intermediateKinds': interKinds}

    # Run the query and process the results
    # we prefer the directed paths, so we run query_directed at first
    print('Try to find a path in the directed graph ...\n')
    records = query_executor.run_query(query_directed, parameters)
    if len(records) == 0:
        print('Can not find a path in the directed graph, try again with undirected graph ...\n')
        records = query_executor.run_query(query_undirected, parameters)
        if len(records) == 0:
            print('Can not find a path in the undirected graph, try src-dest one-step path ...\n')
            records = query_executor.run_query(query_single, parameters)
            if len(records) == 0:
                print('Can not find src-dest one-step path, try src-Namespace-dest path ...\n')
                records = query_executor.run_query(query_namespace, parameters)
    
    # if there are many paths with different lenghts, we prefer the shortest paths (can be more than one path)
    minLen = min([len(record['path']) for record in records])
    metapaths = [record['path'] for record in records if len(record['path']) == minLen]
    
    # Here's how we process and print the paths
    for mp in metapaths:
        print_metapath(path=mp)
    
    return metapaths

def print_metapath(path):
    nodes = path.nodes
    relationships = path.relationships
    # Print details about the nodes
    print("Nodes:")
    for node in nodes:
        #print(f"Node ID: {node.element_id}, Labels: {node.labels}, Properties: {node.items()}")
        print(node['kind'])
    # Print details about the relationships
    print("Relationships:")
    for relationship in relationships:
        #print(f"Relationship ID: {relationship.element_id}, Type: {relationship.type}, Properties: {relationship.items()}")
        print(relationship['type'], relationship['srcKind'], relationship['destKind'], relationship['key'])
    print("----------------------------------")




def find_destKind_relevantResources(errorMessage, srcKind, promptTemplate, rootCauseLocator):
    # replace {involved_object} and {error_message} with actual values 
    prompt = promptTemplate.format(error_message = errorMessage, involved_object=srcKind)
    # add prompt as a message to the thread 
    rootCauseLocator.add_message(prompt)
    # run the Assistant
    rootCauseLocator.run_assistant()
    # check the Run status
    # we can periodically retrieve the Run to check on its status to see if it has moved to completed
    maxPolling = 60
    period = 10
    json_data = {}
    for i in range(1, maxPolling+1):
        time.sleep(period)
        print('polling run result after %d seconds' % (i * period))
        run = rootCauseLocator.get_run_status()
        if(run.status == 'completed'):
            print('run completed')
            # Display the Assistant's Response 
            #messages = rootCauseLocator.get_all_message()
            messages = rootCauseLocator.get_last_message()
            print(messages.data[0])
            json_data = extract_json(messages.data[0].content[0].text.value)
            print(json_data)
            #return json_data
            break
        elif (run.status == 'cancelled'):
            print('run cancelled')
            break
        elif (run.status == 'failed'):
            print('run failed')
            break
        elif (run.status == 'expired'):
            print('run expired')
            break
        elif (i == maxPolling):
            print ('last polling and time out in %d seconds' % period * maxPolling)
    return json_data

def extract_json(message_str):
    json_part = message_str.split('```json')[1].split('```')[0].strip()
    json_data = json.loads(json_part)
    return json_data



def main():
    print("create executor and init connection")
    # Create an instance of the executor class
    metagraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.176:7687", "neo4j", "yong")
    stategraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.174:7687", "neo4j", "yong") 
   
    print('find native and external kinds and build prompt template')
    # find native and external kinds, build prompt template
    nativeKinds, externalKinds = find_native_external_kinds(metagraph_query_executor)
    promptTemplate = build_prompt_template(nativeKinds, externalKinds)
  
    print('create openai client with assistant and thread')
    # create openai client, retrieve assistant and thread
    rootCauseLocator = OpenAIRootCauseLocator()
    rootCauseLocator.retrieve_assistant(assistant_id='asst_RH9XJ35MOG0oaE5cdJwOWaDi')
    rootCauseLocator.retrieve_thread(thread_id='thread_GtGvCyukMtkvZLyDrvaRiD9x')
    
    errorMessages = [ """Error creating: pods "es-white-list-cronjob-1607752440-gprx7" is forbidden: exceeded quota: compute-resources-dumeng1, requested: pods=1, used: pods=50, limited: pods=50""", 
                     """MountVolume.SetUp failed for volume "pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3" : mount failed: exit status 32 Mounting command: systemd-run Mounting arguments: --description=Kubernetes transient mount for /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 --scope -- mount -t nfs 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 Output: Running scope as unit: run-re511f81c07574a6a84df041848b3347f.scope mount.nfs: mounting 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 failed, reason given by server: No such file or directory""",
                     """MountVolume.SetUp failed for volume "es-account-token-k29vm" : secret "es-account-token-k29vm" not found""",
                     """MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found""",
                     """Failed create pod sandbox: rpc error: code = Unknown desc = failed to set up sandbox container "9a71227eb35345ead771b9f20ce90e2402641784c1866710c64adaaf0fbac1f2" network for pod "es-cronjob-1607813100-dqqg6": networkPlugin cni failed to set up pod "es-cronjob-1607813100-dqqg6_fanxy1" network: failed to Statfs "/proc/12631/ns/net": no such file or directory"""

            ]

    for errorMessage in errorMessages:
        print(errorMessage)
        #message = input("Please input the message: \n")

        # find srcKind in stategraph according to message, (Event)-[involvedObject_uid]->(srcKind)
        print('test find_srcKind()')
        srcKind = find_srcKind(stategraph_query_executor, errorMessage)

        print('test find destKind and relevantResources')
        # find destKind and relevantResources with gpt4 assistant 
        destRelevant = find_destKind_relevantResources(errorMessage, srcKind, promptTemplate, rootCauseLocator)

        # find metapaths in metagraph
        print('test find_metapath()')
        destKind = destRelevant['DestinationKind'] 
        relevantResources = destRelevant['RelevantResources']
        #intermediateKinds = [x for x in relevantResources if x not in [srcKind, destKind]] 
        intermediateKinds = [x for x in relevantResources if (x not in [srcKind, destKind]) and (x in nativeKinds or x in externalKinds)]
        
        find_metapath(metagraph_query_executor, srcKind, destKind, intermediateKinds)


    print("close connection")
    # Close the connection when done
    metagraph_query_executor.close()
    stategraph_query_executor.close()

if __name__ == "__main__":
    main()
