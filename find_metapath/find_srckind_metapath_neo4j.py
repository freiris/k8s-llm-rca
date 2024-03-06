#!/usr/bin/env python

'''
import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from neo4j_query_executor import Neo4jQueryExecutor
from openai_root_cause_locator import OpenAIRootCauseLocator
from openai_root_cause_locator import build_prompt_template
'''

import json
from common.openai_generic_assistant import OpenAIGenericAssistant


def setup_root_cause_locator():
    instructions = """As an AI expert in Kubernetes (k8s) systems, you are equipped to understand the various components and API resources involved within a k8s cluster environment, as well as the external systems with which k8s interacts. Your expertise lies in analyzing k8s architectures and providing insightful diagnostic interpretations of the issues these systems might face.

When provided with an error message or log output from a Kubernetes cluster, you are expected to perform the following steps:

Parse and comprehend the given error message, identifying key elements that could be indicative of the underlying issue.

Reference your extensive knowledge of k8s components (e.g., nodes, pods, services, deployments, statefulsets, daemonsets, replication controllers, replica sets, jobs, cronjobs, services, ingresses, network policies, volumes, PersistentVolume (PV), PersistentVolumeClaim (PVC), secrets, configmaps, service accounts, roles, ClusterRoles, RoleBindings, ClusterRoleBindings, etc.) and API resources, as well as your understanding of how they interoperate within a cluster.

Evaluate the context in which the error has occurred, considering the broader k8s system interactions and dependencies, which may include cloud service providers, container networks, storage systems, and other cloud-native projects that might influence k8s functionality.

Use the information gathered from the error message alongside your knowledge of k8s to hypothesize potential root causes of the error. Discuss the interrelation between k8s components that might have contributed to the issue.

Provide a succinct and structured response that outlines possible causes for the error. If appropriate, you may suggest a logical sequence of troubleshooting steps that should be taken to further narrow down the cause and resolve the issue.

If additional information is necessary to pinpoint the problem, advise on what specific data should be collected or which diagnostic commands (such as kubectl commands) should be executed within the k8s environment.

Offer any best practices related to cluster operations, maintenance, and monitoring that could help in preventing such errors in the future or easing the diagnostic process.

Remain neutral in language and refrain from any form of speculation that cannot be substantiated by your embedded knowledge. Provide clear disclaimers if a suggestion is based on common patterns rather than exact diagnostics.

You must not access or interact with any external systems or databases, but rather provide instructions or recommendations based on the error context and your knowledge database as of April 2023.

Keep your output user-friendly and accessible for various skill levels— offer explanations in layman's terms where possible, while also providing technical details for more advanced users when necessary.

Remember to approach each situation as unique, using the information given to you in the error message as a starting point for your expert analysis."""

    name = 'k8s-root-cause-locator'

    rootCauseLocator = OpenAIGenericAssistant()
    rootCauseLocator.create_assistant(instructions, name, 'gpt-4')
    rootCauseLocator.create_thread()
    #rootCauseLocator.retrieve_assistant(assistant_id='asst_RH9XJ35MOG0oaE5cdJwOWaDi')
    #rootCauseLocator.retrieve_thread(thread_id='thread_GtGvCyukMtkvZLyDrvaRiD9x')

    print(rootCauseLocator.assistant.id)
    print(rootCauseLocator.thread.id)
    print(f'https://platform.openai.com/playground?assistant={rootCauseLocator.assistant.id}&thread={rootCauseLocator.thread.id}')
    

    return rootCauseLocator


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
        print(relationship.type, relationship['srcKind'], relationship['destKind'], relationship['key'])
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
    
    messages = rootCauseLocator.wait_get_last_k_message(1)
    json_data = extract_json(messages.data[0].content[0].text.value)

    return json_data

def extract_json(message_str):
    json_part = message_str.split('```json')[1].split('```')[0].strip()
    json_data = json.loads(json_part)
    return json_data



def build_prompt_template(nativeKinds, externalKinds):
    # limit the kinds within the k8s-api-resource and k8s-external-resource kinds in metagraph
    prefix = (
        "The predefined k8s API resource kinds and external resource kinds are the following:\n\n"
        "k8s-api-resource-kinds: {native}\n\n"
        "k8s-external-resource-kinds: {external}\n\n"
    ).format(
        native = ', '.join(nativeKinds),
        external = ', '.join(externalKinds)
    )

    # decribe the steps to perform, use {involved_object} and {error_message} as placeholders
    requirement = (
        "Perform an analysis on the Kubernetes error message that mentions a {involved_object}. "
        "Follow these steps to prepare the analysis:\n\n"
        "1. Recognize the {involved_object} as the starting point of the issue.\n"
        "2. Determine the 'destKind' within specified k8s API resource kinds and k8s external resource kinds "
        "that provides a resolution to the problem.\n"
        "3. Enumerate the most critical k8s API and external resources relevant to the matter "
        "within the predefined kinds.\n"
        "4. Chart the primary progression from {involved_object} to 'destKind', including the most relevant "
        "resources as waypoints.\n"
        "5. Output the findings in JSON format encapsulated within triple backticks and the 'json' specifier for clear demarcation as a code block. The JSON output should not contain additional descriptions and must follow the given structure:\n"
        "```json"
        "{{\n"
        "    'SourceKind': {involved_object},\n"
        "    'DestinationKind': 'destKind', // 'destKind' must be from the predefined resource kinds list\n"
        "    'RelevantResources': ['Resource1', 'Resource2', ..., {involved_object}, 'destKind'],\n"
        "    'PrimaryPath': [\n"
        "                    {{'Edge': 1, 'start': '{involved_object}', 'end': 'Resource1'}},\n"
        "                    {{'Edge': 2, 'start': 'Resource1', 'end': 'Resource2'}},\n"
        "                    ...\n"
        "                    {{'Edge': n, 'start': 'Resource(n-1)', 'end': 'destKind'}}\n"
        "                    ]\n"
        "}}\n"
        "```"
        "Analyze the following error message ensuring 'destKind' and 'Resources-x' are strictly limited to the provided lists:\n\n"
        "{error_message}\n"
    )

    return prefix + requirement

