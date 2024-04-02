#!/usr/bin/env python

import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from neo4j_query_executor import Neo4jQueryExecutor
from openai_cypher_query_generator import OpenAICypherQueryGenerator
from openai_cypher_query_generator import build_generation_template


def setup_cypher_generator():
    cypherQueryGenerator = OpenAICypherQueryGenerator()
    # the create() will timeout, let's use retrieve()
    cypherQueryGenerator.create_assistant()
    #cypherQueryGenerator.retrieve_assistant(assistant_id="asst_IeNQOAKLBC88ELj56pWyNStw")
    cypherQueryGenerator.create_thread()
    #cypherQueryGenerator.retrieve_thread(thread_id="thread_ehx4so43oCQdmeeevUrpvn04")

    label_message = "Let's label the following prompt template as generation-template-1, and use it to generate cypher query later"
    cypherQueryGenerator.add_message(label_message)

    generation_template = build_generation_template()
    cypherQueryGenerator.add_message(generation_template)
    
    return cypherQueryGenerator


def generate_cypher_query(metapath, error_message, cypherQueryGenerator):
    # build the prompt 
    prompt = f"""
    Let's use generation-template-1 and generate a cypher query for the following example. Strictly follow the (srcKind)-[rel]->(destkind) ordering, don't reverse it. Return the generated query in the following format:
    ```cypher
    generated_cypher_query
    ```
    the provided metapath is:
    {metapath}
    the error message to filtering is:
    {error_message}
    """
    cypherQueryGenerator.add_message(prompt)

    print('run assistant')
    cypherQueryGenerator.run_assistant()

    cypher_query = ''

    maxPolling = 60
    period = 10
    for i in range(1, maxPolling+1):
        time.sleep(i * period)
        print('polling run result after %d seconds' % (i * 10))
        
        run = cypherQueryGenerator.get_run_status()
        if(run.status == 'completed'):
            print('run completed')
            messages = cypherQueryGenerator.get_last_message()
            print(messages.data[0].content[0].text.value)
            cypher_query = extract_cypher(messages.data[0].content[0].text.value)
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
    
    return cypher_query

def extract_cypher(message_str):
    cypher_part = message_str.split('```cypher')[1].split('```')[0].strip()
    return cypher_part


def run_and_filter_query(query_executor, cypher_query):
    # the records may contains dest nodes that not mentioned by the EVENT
    records = query_executor.run_query(cypher_query)

    # by default, EVENT is the 2nd element, dest is the last element. 
    # i.e, RETURN event, r1, evt, r2, pod, r3, secret 
    res = []
    for record in records:
        if message_compatible(record):
            res.append(record)
    
    if len(res) == 0:
        print('ALL records are not message compatible')
    
    return res

def message_compatible(record):
    message = record[2]['message']
    dest = record[len(record)-1]
    
    if dest['isNative'] == 'true':
        key = 'name2'
    elif dest['isAtomic'] == 'true':
        key = 'val'
    elif dest['tag'] in ['nfs', 'hostPath']:
        key = 'path'
    elif dest['tag'] == 'container':
        key = 'containerName'
    elif dest['tag'] == 'image':
        key = 'imageName'
    
    return (dest[key] in message)
    

# we expect the time range of EVENT and STATE overlaps
# namely, [E.tmin, E.tmax] ∩ [S.tmin, S.tmax] is not empty
def find_loose_states(entityKind, entityId, tmin, tmax):
    stateKind = entityKind.upper()
    cypher_query = f"""
    MATCH (n1:{entityKind})-[r1:HasState]->(n2:{stateKind})
    WHERE n1.id = '{entityId}'
    AND r1.tmin <= '{tmax}' AND r1.tmax >= '{tmin}'
    RETURN n1, r1, n2
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
    RETURN n1, r1, n2
    LIMIT 10;
    """
    return cypher_query



def check_states_existence_and_semantics(query_executor, cypher_query, semanticAnalyzer, error_message):
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
            record_semantics = check_senmatics(record, error_message, semanticAnalyzer)
            clues.append(record['kind'] + '(' + record['id'] + '): ' + record_semantics)

    return clues


def check_semantics(record, error_message, semanticAnalyzer):
    # pick fileds that are important to check
    important_fields = ['status', 'spec', 'path','server','subsets','roleRef','subjects',\
                        'rules','webhooks','secrets', 'data'] #'metadata' can be ignored 
    common_fields = set(record.keys()).intersection(set(important_fields)) 
    tmp = dict()
    for key in common_fields:
        tmp[key] = record[key]
    # build the prompt
    kind = record['kind']
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
    clue = ''
    maxPolling = 60
    period = 10
    for i in range(1, maxPolling+1):
        time.sleep(i * period)
        print('polling run result after %d seconds' % (i * 10))
        run = cypherQueryGenerator.get_run_status()
        if(run.status == 'completed'):
            print('run completed')
            messages = cypherQueryGenerator.get_last_message()
            print(messages.data[0].content[0].text.value)
            clue = messages.data[0].content[0].text.value
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
    return clue

def main():
    print("create executor and init connection")
    # Create an instance of the executor class
    stategraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.174:7687", "neo4j", "yong")

    print('create openai client with assistant and thread') 
    cypherQueryGenerator = setup_cypher_generator()
    
    '''
    metapath = """
    HasEvent, Event, EVENT, metadata_uid;
    ReferInternal, Event, Pod, involvedObject_uid;
    ReferInternal, Pod, Secret, spec_volumes_secret_secretName;
    """
    
    error_message = """
    MountVolume.SetUp failed for volume "es-account-token-k29vm" : secret "es-account-token-k29vm" not found 
    """

    '''
    metapath = """
    HasEvent, Event, EVENT, metadata_uid;
    ReferInternal, Event, Pod, involvedObject_uid;
    ReferInternal, Pod, PersistentVolumeClaim, spec_volumes_persistentVolumeClaim_claimName;
    ReferInternal, PersistentVolume, PersistentVolumeClaim, spec_claimRef_uid;
    UseExternal, PersistentVolume, nfs, spec_nfs_path
    """

    error_message = """MountVolume.SetUp failed for volume "pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3" : mount failed: exit status 32 Mounting command: systemd-run Mounting arguments: --description=Kubernetes transient mount for /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 --scope -- mount -t nfs 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 Output: Running scope as unit: run-re511f81c07574a6a84df041848b3347f.scope mount.nfs: mounting 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 failed, reason given by server: No such file or directory """
    
    #error_message = """(combined from similar events): MountVolume.SetUp failed for volume "pvc-2435159a-48e8-410b-a047-ded096ec5ce4" : mount failed: exit status 32 Mounting command: systemd-run Mounting arguments: --description=Kubernetes transient mount for /var/lib/kubelet/pods/0b51ae1e-cbaa-4e21-a040-7bc1ef5a7398/volumes/kubernetes.io~nfs/pvc-2435159a-48e8-410b-a047-ded096ec5ce4 --scope -- mount -t nfs 172.16.112.63:/mnt/k8s_nfs_pv/xuw1-ds-gemini-pvc-xuw1-c1-xuw1-c1-0-pvc-2435159a-48e8-410b-a047-ded096ec5ce4 /var/lib/kubelet/pods/0b51ae1e-cbaa-4e21-a040-7bc1ef5a7398/volumes/kubernetes.io~nfs/pvc-2435159a-48e8-410b-a047-ded096ec5ce4 Output: Running scope as unit: run-rc8f00a9439c04695b1698fa4edd7f3c7.scope mount.nfs: mounting 172.16.112.63:/mnt/k8s_nfs_pv/xuw1-ds-gemini-pvc-xuw1-c1-xuw1-c1-0-pvc-2435159a-48e8-410b-a047-ded096ec5ce4 failed, reason given by server: No such file or directory """
    #'''
    
    cypher_query = generate_cypher_query(metapath, error_message, cypherQueryGenerator)

    #records = stategraph_query_executor.run_query(cypher_query)
    records = run_and_filter_query(stategraph_query_executor, cypher_query)    

    for x in records:
        print(x)


if __name__ == "__main__":
    main()

