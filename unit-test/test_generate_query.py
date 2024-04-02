#!/usr/bin/env python
import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from common.neo4j_query_executor import Neo4jQueryExecutor
from common.openai_generic_assistant import OpenAIGenericAssistant
from generate_query.generate_query import *


def main():
    print("create executor and init connection")
    # Create an instance of the executor class
    stategraph_query_executor = Neo4jQueryExecutor("bolt://10.1.0.174:7687", "neo4j", "yong")

    print('create openai client with assistant and thread')
    cypherQueryGenerator = setup_cypher_generator()

    #'''
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




