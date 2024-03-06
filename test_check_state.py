#!/usr/bin/env python

import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from common.neo4j_query_executor import Neo4jQueryExecutor
from common.openai_generic_assistant import OpenAIGenericAssistant
from check_state.analyze_root_cause import * 

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


