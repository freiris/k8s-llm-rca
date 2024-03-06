#!/usr/bin/env python

import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from neo4j_query_executor import Neo4jQueryExecutor
from openai_generic_assistant import OpenAIGenericAssistant


def setup_state_semantic_analyzer():
    instructions = 'You are an expert in k8s, and can find the mistakes in the state, and can further determine whether the mistakes is related to the error message'
    name = 'k8s-state-semantic-analyzer'
    semanticAnalyzer = OpenAIGenericAssistant()
    semanticAnalyzer.create_assistant(instructions, name, 'gpt-4')
    semanticAnalyzer.create_thread()
    
    task_prompt = """
    You will receive two separate pieces of information:
    1. A JSON string that represents the current state of a Kubernetes (k8s) object, which varies in type (e.g., PersistentVolume is one example).
    2. An error message that may or may not be associated with the k8s object.

    Your task involves multiple steps:
    - First, parse the provided JSON string to extract and examine the object's details.
    - Focus your scrutiny on the 'spec' and 'status' fields within the JSON structure.
        - If either the 'spec' or 'status' field is not present, direct your attention to other significant fields in the JSON that could provide valuable insight.
    - Conduct an evaluation to determine if there are any apparent misconfigurations or errors in the JSON fields, especially those which could align with the nature of the provided error message.
    - If the error message seems to relate to the JSON data, clarify the connection and identify any anomalies or errors in the data.
    - If the error message appears to be unrelated to the k8s object's state, acknowledge this finding.
    - Provide a summary of any issues discovered with the k8s JSON data.

    Proceed with these instructions when prompted with the k8s object's JSON string and error message.
    """
    
    semanticAnalyzer.add_message(task_prompt)

    return semanticAnalyzer


# we expect the time range of EVENT and STATE overlaps
# namely, [E.tmin, E.tmax] ∩ [S.tmin, S.tmax] is not empty
def find_loose_states(entityKind, entityId, tmin, tmax):
    stateKind = entityKind.upper()
    cypher_query = f"""
    MATCH (n1:{entityKind})-[r1:HasState]->(n2:{stateKind})
    WHERE n1.id = '{entityId}'
    AND r1.tmin <= '{tmax}' AND r1.tmax >= '{tmin}'
    RETURN n2
    LIMIT 10;
    """
    return cypher_query

# we expect the EVENT happens within the time range of STATE
# namely, timestamp in [tmin, tmax]
def find_strict_states(entityKind, entityId, timestamp):
    stateKind = entityKind.upper()
    cypher_query = f"""
    MATCH (n1:{entityKind})-[r1:HasState]->(n2:{stateKind})
    WHERE n1.id = '{entityId}'
    AND r1.tmin <= '{timestamp}' AND r1.tmax >= '{timestamp}'
    RETURN n2
    LIMIT 10;
    """
    return cypher_query



def check_states_existence_and_semantic(query_executor, cypher_query, semanticAnalyzer, error_message):
    clues = []
    records = query_executor.run_query(cypher_query)
    # step1: check whether the STATE node exist
    if len(records) == 0:
        stateNotExist = 'There is not a STATE node corresponds to the Entity node' 
        clues.append(stateNotExist)
        print(stateNotExist)
    # step2: check the content of STATE node with gpt4 using semantic analysis     
    else:
        for record in records:
            state_node = record['n2']
            state_node_semantic = check_semantic(state_node, error_message, semanticAnalyzer)
            clues.append(state_node['kind'] + '(' + state_node['id'] + '): ' + state_node_semantic)

    return clues


def check_semantic(state_node, error_message, semanticAnalyzer):
    # pick fileds that are important to check
    important_fields = ['status', 'spec', 'path','server','subsets','roleRef','subjects',\
                        'rules','webhooks','secrets', 'data'] #'metadata' can be ignored 
    common_fields = set(state_node.keys()).intersection(set(important_fields)) 
    tmp = dict()
    for key in common_fields:
        tmp[key] = state_node[key]
    # build the prompt
    kind = state_node['kind']
    prompt = f"""
    The following JSON comes from a {kind} object. Focus on the 'spec' and 'status' fields
    (or other relevant fields if 'spec' and 'status' are not present) to find some clues for 
    the following error message:\n{error_message}?\nThe JSON is:\n{tmp}
    """
    print(prompt)
   
   # add message and run assistant 
    semanticAnalyzer.add_message(prompt)
    print('run assistant')
    semanticAnalyzer.run_assistant()
   
    messages = semanticAnalyzer.wait_get_last_k_message(1)
    clue = messages.data[0].content[0].text.value
    
    return clue


def main():
    print("create executor and init connection")
    # Create an instance of the executor class
    stategraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.174:7687", "neo4j", "yong")
    semanticAnalyzer = setup_state_semantic_analyzer()    

    # NoSuchFileDir example
    '''
    #entityKind = 'PersistentVolume'
    #entityId = '903b2237-03b0-46a6-8107-488bd312e52e'
    
    entityKind = 'PersistentVolumeClaim'
    entityId = 'f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3'

    timestamp = '2020-12-13 15:30:02.013'
    
    #tmin = '2020-12-13 15:30:02.013'
    #tmax = '2020-12-13 16:25:02.013'

    cypher_query = find_strict_states(entityKind, entityId, timestamp)
    #cypher_query = find_loose_states(entityKind, entityId, tmin, tmax) 

    error_message = """MountVolume.SetUp failed for volume "pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3" : mount failed: exit status 32 Mounting command: systemd-run Mounting arguments: --description=Kubernetes transient mount for /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 --scope -- mount -t nfs 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 Output: Running scope as unit: run-re511f81c07574a6a84df041848b3347f.scope mount.nfs: mounting 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 failed, reason given by server: No such file or directory"""
    '''

    # ExceedQuota example
    entityKind = 'ResourceQuota'
    entityId = '66637a34-b552-448f-9da6-976aa7462533'
    timestamp = '2020-12-11 06:35:02.011'
    
    cypher_query = find_strict_states(entityKind, entityId, timestamp)
    
    error_message = """(combined from similar events): Error creating: pods "console-white-list-cronjob-1607661480-2v28l" is forbidden: exceeded quota: compute-resources-zhangxianqing1, requested: pods=1, used: pods=50, limited: pods=50"""


    clues = check_states_existence_and_semantic(stategraph_query_executor, cypher_query, semanticAnalyzer, error_message)

    for clue in clues:
        print(clue)

if __name__ == "__main__":
    main()

