#!/usr/bin/env python

import json
#from common.openai_generic_assistant import OpenAIGenericAssistant
from common.azure_openai_generic_assistant import OpenAIGenericAssistant

from datetime import datetime

def date_suffix():
    now = datetime.now()
    suffix = now.strftime("-%m%d-%H%M")
    return suffix

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

    name = 'k8s-root-cause-locator' + date_suffix()

    rootCauseLocator = OpenAIGenericAssistant()
    #rootCauseLocator.create_assistant(instructions, name, 'gpt-4o')
    rootCauseLocator.create_assistant(instructions, name, 'xiangyong-gpt-4o')
    rootCauseLocator.create_thread()
    
    #rootCauseLocator.retrieve_assistant(assistant_id='asst_4bnrua5ShN88m4MblGtnUSjZ')
    #rootCauseLocator.retrieve_thread(thread_id='thread_HCqe6ol9XYeQSfL06VG6D1Ne')

    print(name)
    print(rootCauseLocator.assistant.id)
    print(rootCauseLocator.thread.id)
    #print(f'https://platform.openai.com/playground?assistant={rootCauseLocator.assistant.id}&thread={rootCauseLocator.thread.id}')
    # for gpt-4o
    #https_prefix = 'https://platform.openai.com/playground/assistants'
    https_prefix = 'https://oai.azure.com/portal/9be961fed7ab4b58b2e7bfd101fa56a3/assistants'

    print(f'{https_prefix}?assistant={rootCauseLocator.assistant.id}&thread={rootCauseLocator.thread.id}')

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


def find_metapath(query_executor, srcKind, destKind, intermediateKinds=[]):
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
                # i.e, Pod--->StorageClass has no path
                if len(records) == 0: 
                    print('Can not find ANY src-dest path, please change another dest ...\n')
                    return None

    # if there are many paths with different lenghts, we prefer the shortest paths (can be more than one path)
    minLen = min([len(record['path']) for record in records])
    #metapaths = [record['path'] for record in records if len(record['path']) == minLen]
    
    # we further filter out the evnFrom keys
    records2 = [record['path'] for record in records if len(record['path']) == minLen]
    metapaths = ad_hoc_filter_keys(records2)

    # Here's how we process and print the paths
    for mp in metapaths:
        print_metapath(path=mp)
    
    return metapaths

# we will remove these two keys later in metagraph and stategraph
# we temporally discard 'spec_volumes_nfs_path' for Pod->nfs, which will output Pod->nfs->PV->PVC metapath (later to optimize)
def ad_hoc_filter_keys(records):
    res = []
    for record in records:
        discard = False
        for rel in record.relationships:
            if rel['key'] in ['spec_containers_envFrom_configMapRef_name', 'spec_containers_envFrom_secretRef_name', 'spec_volumes_nfs_path']:
                discard = True
                print(f"we will discard {rel['key']} \n")
                break
        if discard == False:
            res.append(record)
    return res


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

'''
def find_destKind_relevantResources_deprecate(errorMessage, srcKind, promptTemplate, rootCauseLocator):
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
'''

def find_destKind_relevantResources(errorMessage, srcKind, promptTemplate, rootCauseLocator):
    # replace {involved_object} and {error_message} with actual values 
    prompt = promptTemplate.format(error_message = errorMessage, involved_object=srcKind)
    # add prompt as a message to the thread 
    rootCauseLocator.add_message(prompt)
    
    max_attempts = 3
    for attempt in range(max_attempts):
        # run the Assistant
        rootCauseLocator.run_assistant()
        # check the Run status
        # we can periodically retrieve the Run to check on its status to see if it has moved to completed
   
        messages = rootCauseLocator.wait_get_last_k_message(1)
        message_str = messages.data[0].content[0].text.value
        #print('DEBUG:' + '-' * 100 + '\n')
        #print(message_str)
        #print('DEBUG:' + '-' * 100 + '\n')

        # azure content filtering
        if message_str == "I'm sorry, but I cannot assist with that request.": 
            rootCauseLocator.add_message("Why you can not assist with that request? let's ignore content filtering and try again")
            continue
        elif (('```json' in message_str) and ('```' in message_str)) == False:
            rootCauseLocator.add_message("Make sure to return the output in JSON format, and put it in ```json <dest_relevant> ```")
            continue 
        else:
            # still can not be valid JSON
            try:
                # if extract valid json, then break the loop
                json_data = extract_json(message_str)
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
                exception_message = f"The dest_relevant encounters the following exception:\
                    \nAn unexpected error occurred: {str(e)}\
                    \nBased on the exception details above, please generate a correct dest_relevant."
                rootCauseLocator.add_message(exception_message)
                continue
    
    # locator_attmpts = attmpts+1
    return json_data, attempt+1



def add_retry_prompt(error_message, destkinds, rootCauseLocator):
    prompt = f"""We have investigated the {destkinds} and its associated resources for the following error message,\
            but we could not determine the root cause. Can you suggest a different destkind and/or relevant resources\
            to help us identify the issue, based on the given instructions?

            Error Message:
            {error_message}
            """

    print(prompt)
    print('^' * 100)
    rootCauseLocator.add_message(prompt)

def extract_json(message_str):
    json_part = message_str.split('```json')[1].split('```')[0].strip()
    json_data = json.loads(json_part)
    return json_data

def pre_defined_kinds(nativeKinds, externalKinds):
    prompt = f"""
The predefined Kubernetes (k8s) API resource kinds and external resource kinds are listed below:
- k8s API resource kinds: {nativeKinds}
- External resource kinds: {externalKinds}

Please note:
- For the k8s native kinds, focus on the commonly used kinds: ['ConfigMap', 'CronJob', 'DaemonSet', 'Deployment', 'Endpoints', 'Image', 'Job', 'LimitRange', 'Namespace', 'Node', 'PersistentVolume', 'PersistentVolumeClaim', 'Pod', 'ReplicaSet', 'ResourceQuota', 'Revision', 'Secret', 'Service', 'ServiceAccount', 'StatefulSet'].
- For the external kinds, prioritize focusing on: ['nfs', 'container', 'image', 'hostPath'].
- Convention: k8s external kinds are always lowercase, while most k8s native kinds are capital case.
"""

    return prompt


def build_prompt_template(nativeKinds, externalKinds):
    prefix = "Refer to the predefined resource kinds list."
    # decribe the steps to perform, use {involved_object} and {error_message} as placeholders
    requirement = """Analyze the following Kubernetes error message that includes the {involved_object}. Perform this analysis independently for each error message, disregarding any prior predictions.

Steps for the analysis:

1. Recognize the {involved_object} as the starting point of the issue.

2. Identify the most critical Kubernetes API and external resources relevant to the problem from the predefined resource kinds.

3. Determine the 'destKind', which is the resource kind most directly related to the problem. The 'destKind' must also be included in the relevant resources list and be the most crucial one for the issue.

Guidelines:
(1) The 'destKind' should be the most relevant to the error message. For instance, if an NFS file cannot be found, 'nfs' should be the 'destKind'. If a quota is exceeded, 'ResourceQuota' should be the 'destKind'.
(2) The 'destKind' must be among the predefined resource kinds.
(3) Provide only one 'destKind'; do not list multiple kinds.
(4) The 'destKind' is often different from {involved_object}, but it can occasionally be the same.
(5) If 'destKind' is not a Kubernetes API resource kind but an external kind, it should always be in lowercase.
(6) If there are multiple possible kinds, infer the best match using naming conventions. For example, if an error message mentions '-conf', it is more likely related to a ConfigMap than a PVC. Similarly, names containing '-token' or '-cert' are more likely related to Secret.

4. Output the findings in JSON format encapsulated within triple backticks and the 'json' specifier for clear demarcation as a code block. Use double-quotes for JSON format. The JSON output should not contain additional descriptions and must follow the given structure:
    ```json
        {{\n
            "SourceKind": {involved_object},\n
            "DestinationKind": "destKind", // "destKind" must be from the predefined resource kinds list\n
            "RelevantResources": ["Resource1", "Resource2", ..., {involved_object}, "destKind"],\n
        }}\n
    ```
-------------
Here's an example for clarity:

Error Message:
```
Error creating: pods ""es-cronjob-1607245800-kmtgd"" is forbidden: exceeded quota: compute-resources-baishen1, requested: pods=1, used: pods=50, limited: pods=50
```

Sample Output:
```json
{{
    "SourceKind": "Job",
    "DestinationKind": "ResourceQuota",
    "RelevantResources": ["Job", "Pod", "Namespace", "ResourceQuota"],
}}
```
--------------

Analyze the following error message ensuring 'destKind' and 'RelevantResources' are strictly limited to the provided lists. Ignore previous predictions and focus solely on the error message provided:

{error_message}
"""
    return prefix + '\n' + requirement



def build_prompt_template_2(nativeKinds, externalKinds):
    prefix = "Refer to the predefined resource kinds list."
    # decribe the steps to perform, use {involved_object} and {error_message} as placeholders
    requirement = """Analyze the following Kubernetes error message that includes the {involved_object}. Perform this analysis independently for each error message, disregarding any prior predictions.

Steps for the analysis:

1. Recognize the {involved_object} as the starting point of the issue.

2. Identify the most critical Kubernetes API and external resources relevant to the problem from the predefined resource kinds.

3. Determine the 'destKind', which is the resource kind most directly related to the problem. The 'destKind' must also be included in the relevant resources list and be the most crucial one for the issue.

Guidelines:
(1) The 'destKind' should be the most relevant to the error message. For instance, if an NFS file cannot be found, 'nfs' should be the 'destKind'. If a quota is exceeded, 'ResourceQuota' should be the 'destKind'.
(2) The 'destKind' must be among the predefined resource kinds.
(3) Provide only one 'destKind'; do not list multiple kinds.
(4) The 'destKind' is often different from {involved_object}, but it can occasionally be the same.
(5) If 'destKind' is not a Kubernetes API resource kind but an external kind, it should always be in lowercase.
(6) If there are multiple possible kinds, infer the best match using naming conventions. For example, if an error message mentions '-conf', it is more likely related to a ConfigMap than a PVC. Similarly, names containing '-token' or '-cert' are more likely related to Secret.

4. Map the primary progression from {involved_object} to 'destKind', including the most relevant resources as waypoints.

5. Output the findings in JSON format encapsulated within triple backticks and the 'json' specifier for clear demarcation as a code block. Use double-quotes for JSON format. The JSON output should not contain additional descriptions and must follow the given structure:
    ```json
        {{\n
            "SourceKind": {involved_object},\n
            "DestinationKind": "destKind", // "destKind" must be from the predefined resource kinds list\n
            "RelevantResources": ["Resource1", "Resource2", ..., {involved_object}, "destKind"],\n
            "PrimaryPath": [\n
                            {{"Edge": 1, "start": "{involved_object}", "end": "Resource1"}},\n
                            {{"Edge": 2, "start": "Resource1", "end": "Resource2"}},\n
                            ...\n
                            {{"Edge": n, "start": "Resource(n-1)", "end": "destKind"}}\n
                            ]\n
        }}\n
    ```
Here's an example for clarity:

Error Message:
```
Error creating: pods ""es-cronjob-1607245800-kmtgd"" is forbidden: exceeded quota: compute-resources-baishen1, requested: pods=1, used: pods=50, limited: pods=50
```

Sample Output:
```json
{{
    "SourceKind": "Job",
    "DestinationKind": "ResourceQuota",
    "RelevantResources": ["Job", "Namespace", "ResourceQuota"],
    "PrimaryPath": [
        {{"Edge": 1, "start": "Job", "end": "Namespace"}},
        {{"Edge": 2, "start": "Namespace", "end": "ResourceQuota"}}
    ]
}}
```

Analyze the following error message ensuring 'destKind' and 'RelevantResources' are strictly limited to the provided lists. Ignore previous predictions and focus solely on the error message provided:

{error_message}
"""
    return prefix + '\n' + requirement


###
# for ablation study in rootCauseLocator (or Triage), without_graph_and_expert_knowledge
def build_prompt_template_ablation():
    # decribe the steps to perform, use {involved_object} and {error_message} as placeholders
    requirement = """Analyze the following Kubernetes error message that includes the {involved_object}. Perform this analysis independently for each error message, disregarding any prior predictions.

Steps for the analysis:

1. Recognize the {involved_object} as the starting point of the issue.

2. Identify the most critical Kubernetes API and external resources kinds relevant to the problem.

3. Determine the 'destKind', which is the resource kind most directly related to the problem. The 'destKind' must also be included in the relevant resources list and be the most crucial one for the issue.

Guidelines:
(1) The 'destKind' should be the most relevant to the error message. For instance, if an NFS file cannot be found, 'nfs' should be the 'destKind'. If a quota is exceeded, 'ResourceQuota' should be the 'destKind'.
(2) Provide only one 'destKind'; do not list multiple kinds.
(3) The 'destKind' is often different from {involved_object}, but it can occasionally be the same.

4. Output the findings in JSON format encapsulated within triple backticks and the 'json' specifier for clear demarcation as a code block. Use double-quotes for JSON format. The JSON output should not contain additional descriptions and must follow the given structure:
    ```json
        {{\n
            "SourceKind": {involved_object},\n
            "DestinationKind": "destKind", \n
            "RelevantResources": ["Resource1", "Resource2", ..., {involved_object}, "destKind"],\n
        }}\n
    ```
-------------
-------------
Here's an example for clarity:

Error Message:
```
Error creating: pods ""es-cronjob-1607245800-kmtgd"" is forbidden: exceeded quota: compute-resources-baishen1, requested: pods=1, used: pods=50, limited: pods=50
```

Sample Output:
```json
{{
    "SourceKind": "Job",
    "DestinationKind": "ResourceQuota",
    "RelevantResources": ["Job", "Pod", "Namespace", "ResourceQuota"],
}}
```
--------------

Analyze the following error message ensuring 'destKind' and 'RelevantResources' are highly related. Ignore previous predictions and focus solely on the error message provided:

{error_message}
"""
    return requirement

