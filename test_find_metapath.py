#!/usr/bin/env python

import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from common.neo4j_query_executor import Neo4jQueryExecutor
#from openai_root_cause_locator import OpenAIRootCauseLocator
#from openai_root_cause_locator import build_prompt_template

from find_metapath.find_srckind_metapath_neo4j import *


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
    #rootCauseLocator = OpenAIRootCauseLocator()
    #rootCauseLocator.retrieve_assistant(assistant_id='asst_RH9XJ35MOG0oaE5cdJwOWaDi')
    #rootCauseLocator.retrieve_thread(thread_id='thread_GtGvCyukMtkvZLyDrvaRiD9x')
    
    rootCauseLocator = setup_root_cause_locator()

    errorMessages = [ """Error creating: pods "es-white-list-cronjob-1607752440-gprx7" is forbidden: exceeded quota: compute-resources-dumeng1, requested: pods=1, used: pods=50, limited: pods=50""",
                     """MountVolume.SetUp failed for volume "pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3" : mount failed: exit status 32 Mounting command: systemd-run Mounting arguments: --description=Kubernetes transient mount for /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 --scope -- mount -t nfs 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 /var/lib/kubelet/pods/92f33868-35c6-487f-8631-b2206363510a/volumes/kubernetes.io~nfs/pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 Output: Running scope as unit: run-re511f81c07574a6a84df041848b3347f.scope mount.nfs: mounting 172.16.112.63:/mnt/k8s_nfs_pv/chongni1-common-redis-pvc-0-common-redis-0-0-pvc-f3788c43-6ca2-42fa-a1b5-7e760b6c4ff3 failed, reason given by server: No such file or directory""",
                     """MountVolume.SetUp failed for volume "es-account-token-k29vm" : secret "es-account-token-k29vm" not found""",
                     """MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found""",
                     """Failed create pod sandbox: rpc error: code = Unknown desc = failed to set up sandbox container "9a71227eb35345ead771b9f20ce90e2402641784c1866710c64adaaf0fbac1f2" network for pod "es-cronjob-1607813100-dqqg6": networkPlugin cni failed to set up pod "es-cronjob-1607813100-dqqg6_fanxy1" network: failed to Statfs "/proc/12631/ns/net": no such file or directory"""

            ]

    for errorMessage in errorMessages[0:1]:
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


