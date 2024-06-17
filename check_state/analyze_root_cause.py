#!/usr/bin/env python

import neo4j
from common.openai_generic_assistant import OpenAIGenericAssistant
from datetime import datetime

def date_suffix():
    now = datetime.now()
    suffix = now.strftime("-%m%d-%H%M")
    return suffix

def setup_state_semantic_analyzer():
    instructions = """You are an expert in Kubernetes (k8s). Your role involves identifying mistakes in the state and determining whether these mistakes are related to the error message, then summarizing your findings into a report. 
    
    You have two types of tasks:
    1. state-analysis: For these tasks, provide your response directly in plain text, without any JSON formatting.
    2. report-generation: For these tasks, follow the specific JSON format provided in the task prompt.

    Ensure that the JSON formatting used in report-generation tasks does not interfere with the responses for state-analysis tasks."""

    name = 'k8s-state-semantic-analyzer' + date_suffix() 
    
    semanticAnalyzer = OpenAIGenericAssistant()
    semanticAnalyzer.create_assistant(instructions, name, 'gpt-4o')
    semanticAnalyzer.create_thread()
   
    #semanticAnalyzer.retrieve_assistant(assistant_id='asst_N6J0RvH9T5ZowQCJnGGgKFng')
    #semanticAnalyzer.retrieve_thread(thread_id='thread_HWX12Uecgy6n6tPoBbdXjXdJ')

    print(name)
    print(semanticAnalyzer.assistant.id)
    print(semanticAnalyzer.thread.id)
    print(f'https://platform.openai.com/playground?assistant={semanticAnalyzer.assistant.id}&thread={semanticAnalyzer.thread.id}')

    state_rule = """For state-analysis task: In a Kubernetes system, each entity should have a corresponding STATE node which represents its existence and status. If an entity lacks a corresponding STATE node, it signifies a clear error, implying that this entity does not exist or its creation was unsuccessful. This is a fundamental principle that applies across various entities, including but not limited to, nfs (directory in Network File System), Secrets, and ConfigMaps. Therefore, as a best practice, always ensure that all entities have their respective STATE nodes to avoid such errors and maintain the system's robustness and performance."""

    semanticAnalyzer.add_message(state_rule)

    state_task_prompt = """For state-analysis tasks: You will receive two separate pieces of information:
    1. A JSON string that represents the current state of a Kubernetes (k8s) object, which varies in type (e.g., PersistentVolume is one example). 
    2. An error message that may or may not be associated with the k8s object.

    Your task involves multiple steps:
    - Parse the provided JSON string to extract and examine the object's details.
    
    - Focus your scrutiny on the 'spec' and 'status' fields within the JSON structure.
        - If either the 'spec' or 'status' field is not present, direct your attention to other significant fields in the JSON that could provide valuable insight.
    
    - Conduct an evaluation to determine if there are any apparent misconfigurations or errors in the JSON fields, especially those which could align with the nature of the provided error message.
        - If the error message seems to relate to the JSON data, clarify the connection and identify any anomalies or errors in the data.
        - If the error message appears to be unrelated to the k8s object's state, acknowledge this finding.
        - **Crucial Rule**: Your analysis must strictly adhere to the provided factual JSON data. Do NOT create or fabricate any new JSON snippets. If the provided JSON cannot explain the error message, you must clearly state this. Under no circumstances should you invent any JSON data to explain the error message.

    - Summarize key observations and any issues discovered with the k8s JSON data in a concise manner (limit within 200 words).
    Key points to include:
    1. **Key Observations**: Highlight the most relevant findings related to the error message.
    2. **Summary of Issues**: Summarize any issues discovered within the JSON data that contribute to the error message.
    3. **Relevant JSON Fragments**: Include only the most relevant parts or fragments from the k8s JSON that are crucial for understanding the issue. Keep these fragments as short as possible and put together. Do not fabricate or create new JSON snippets; strictly use the data provided. If the data does not explain the error, state this clearly. Do not include the error message.
    
    Your analysis should solely focus on these key points and avoid a step-by-step description or restating the parsed JSON and error message.
    Proceed with these instructions when prompted with the k8s object's JSON string and error message.
    
    Let's label the prompt as 'state_analysis_task_prompt' for later reference."""

    semanticAnalyzer.add_message(state_task_prompt)

    return semanticAnalyzer


# we expect the time range of EVENT and STATE overlaps
# namely, [E.tmin, E.tmax) ∩ [S.tmin, S.tmax) is not empty
def find_loose_states(entityKind, entityId, tmin, tmax):
    stateKind = entityKind.upper()
    cypher_query = f"""
    MATCH (n1:{entityKind})-[r1:HasState]->(n2:{stateKind})
    WHERE n1.id = '{entityId}'
    AND r1.tmin <= '{tmax}' AND r1.tmax > '{tmin}'
    RETURN n2
    LIMIT 10;
    """
    return cypher_query

# we expect the EVENT happens within the time range of STATE
# namely, timestamp in [tmin, tmax)
# we use left-close-right-open range for two reasons:
# (1) we are sure that at tmin, the STATE exists, but we only sure it exists at tmax-1, 
#     not sure at tmax
# (2) we can avoid the timestamp fits in two time-ranges, for example
#     5 in [3, 5] and [5, 8], but 5 not in [3, 5), only in [5, 8)

def find_strict_states(entityKind, entityId, timestamp):
    stateKind = entityKind.upper()
    cypher_query = f"""
    MATCH (n1:{entityKind})-[r1:HasState]->(n2:{stateKind})
    WHERE n1.id = '{entityId}'
    AND r1.tmin <= '{timestamp}' AND r1.tmax > '{timestamp}'
    RETURN n2
    LIMIT 10;
    """
    return cypher_query


def build_report_prompt(kinds):
    # task to perform
    prompt_task = """For report-generation task: Based on the previous analysis of {kinds}, summarize the root cause of the error message, and pinpoint out the most relevant parts.

    1. For each kind, faithfully summarize the findings based on evidences/facts only, and don't include any suspections that are not verified. Then provide a score (0~10/10) to indicate how relevant it is to the error message.

    2. Moreover, provide a resolution for the error with kubectl or bash command if appliable. Note: include crucial details such as resource names, IDs, and numbers that are pertinent to understanding the cause. The kubectl/bash command should incorporate the actual resource names, or namespaces, to achieve precision in execution.

    3. Furthermore, provide an overall score (0~10/10) to indicate how well the conclusion (and detailed summary if needed) can explain the root cause of the error message. Also, determine if further investigation is needed.

    Scoring and Investigation Criteria:
    1. If the conclusion alone can directly explain the root cause of the error message, score it above 9/10, and set "further_investigation": False.
    2. If the conclusion cannot solely explain the root cause, but in combination with a detailed summary, they together can explain the root cause, score it above 7/10 and set "further_investigation": False.
    3. If the conclusion and summary can only partially explain the root cause or are merely relevant to it, score it below 5/10 and set "further_investigation": True."""

    # output format
    prompt_output = """Format the report strictly in the following JSON structure, ensuring that the output contains only the JSON object and no additional text or explanation:
    {
    "summary":[
            {
            "kind": "<k8s object kind>",
            "explanation": "<brief summary of the explanation, include specific evidence for the error if appliable>",
            "relevance_score": "<relevance_score>"
            },
            ....
            ]
    "conclusion": "<summary of the overall findings>"
    "resolution": "<actions to resolve the error, with kubectl/bash command>"
    "overall_score": "<overall score of how much the conclusion can explain root cause of error message>"
    "further_investigation": "<True/False>"
    }

    **The 'kind' should be strictly within the previous analyzed kinds. Do not change it to upper or lower case or make any other modifications.**
    **Provide only the JSON object as the response without any headers, explanations, or additional information outside of the JSON structure.**
    **Use the JSON format only for report-generation task, respond according to the context and requirements provided for each other specific task.**"""

    prompt = prompt_task + prompt_output
    return prompt

def build_report_for_empty_statepath(destKind, error_message, semanticAnalyzer):
    finding = f'We can not find a path for the metapath, and confirmed that there is not a {destKind} entity, which is an obvious error.'
    analysis = f'we analyzed the following error message: \n\
                {error_message} \n\
                and find out:\n\
                {finding}'
    semanticAnalyzer.add_message(analysis)
    
    prompt = build_report_prompt(destKind)
    semanticAnalyzer.add_message(prompt)

    print('run assistant')
    semanticAnalyzer.run_assistant()
    messages = semanticAnalyzer.wait_get_last_k_message(1)
    report = messages.data[0].content[0].text.value
    
    return report, finding

# the statepath is a neo4j record returned by running query for metapath
def check_statepath(query_executor, semanticAnalyzer, statepath):
    # get timestamp, tmin, tmax, error_message from EVENT node
    for ele in statepath:
        if isinstance(ele, neo4j.graph.Node) and (ele['kind'] == 'Event'):
            timestamp = ele['timestamp']
            #tmin = timestamp
            #tmax = ele['nextTimestamp']
            error_message = ele['message']
    
    # check the state of entity node, except Event node (note: 'kind' and 'kind2' are different keys) 
    path_clues = dict()
    # state['kind'] == native_entity['kind2'], i.e, Pod['kind2'] == POD['kind'] == 'Pod'
    # state['kind'] == external_entity['tag'], i.e. nfs['tag'] == NFS['kind'] == 'nfs'
    kind2_tags = []
    for ele in statepath:
        if isinstance(ele, neo4j.graph.Node) and not ((ele['kind2'] == 'Event') or (ele['kind'] == 'Event')):
            entity_kind, entity_name, entity_id = get_kind_name_id(ele)
            print(entity_kind, entity_name, entity_id)
            kind2_tags.append(entity_kind)
            
            '''
            cypher_query = find_strict_states(entity_kind, entity_id, timestamp)
            node_clues = check_states_existence_and_semantic(query_executor, cypher_query,\
                            semanticAnalyzer, error_message)
            '''
            node_clues = check_states_of_entity(entity_kind, entity_name, entity_id, error_message, timestamp,\
                            query_executor, semanticAnalyzer)
            #path_clues[entity_id] = node_clues
            path_clues[f'{entity_kind}({entity_id})'] = node_clues

    # summarize the node clues, make a conclusion, and provide a resolution
    kinds = (', ').join(kind2_tags)
    prompt = build_report_prompt(kinds)
    
    # add message and run assistant 
    semanticAnalyzer.add_message(prompt)
    
    print('run assistant')
    semanticAnalyzer.run_assistant()
    messages = semanticAnalyzer.wait_get_last_k_message(1)
    report = messages.data[0].content[0].text.value 
    
    # the report provide a summary, the path_clues provide details
    return report, path_clues


# check the existence and semantic of the STATE node for an Entity node
def check_states_of_entity(entity_kind, entity_name, entity_id, error_message, timestamp,\
                    query_executor, semanticAnalyzer):
    # generate cypher_query and retrieve records
    cypher_query = find_strict_states(entity_kind, entity_id, timestamp) 
    records = query_executor.run_query(cypher_query)
    
    # check whether the STATE node exist
    clues = []
    if len(records) == 0:
        state_not_exist = f"{entity_kind} ({entity_id}): there is not a STATE ({entity_kind.upper()}) node corresponds to the Entity ({entity_kind}) node, which is an apparent error. we confirm that '{entity_name}' does not exist"
        clues.append(state_not_exist)
        semanticAnalyzer.add_message(state_not_exist)
    # check the content of the STATE node with gpt-4 using semantic analysis
    else:
        for record in records:
            # explicitly use state-task to avoid output format confused by report-task
            semanticAnalyzer.add_message("let's use 'state_analysis_task_prompt' for the following state-analysis task")
            state_node = record['n2']
            state_node_semantic = check_semantic(state_node, error_message, semanticAnalyzer)
            clues.append(state_node['kind'].upper() + '(' + state_node['id'] + '): ' + state_node_semantic)
   
    for clue in clues:
        print('~' * 100)
        print(clue)
        print('~' * 100)

    return clues


def get_kind_name_id(entity):
    # kind
    if entity['isNative'] == 'true':
        kind = entity['kind2']
    elif entity['isNative'] == 'false':
        kind = entity['tag']
    # name 
    if entity['isNative'] == 'true':
        key = 'name2'
    elif entity['isAtomic'] == 'true':
        key = 'val'
    elif entity['tag'] in ['nfs', 'hostPath']:
        key = 'path'
    elif entity['tag'] == 'container':
        key = 'containerName'
    elif entity['tag'] == 'image':
        key = 'imageName'
    name = entity[key]
    # id 
    uid = entity['id']
    
    return kind, name, uid

def check_semantic(state_node, error_message, semanticAnalyzer):
    # pick fileds that are important to check
    important_fields = ['status', 'spec', 'path','server','subsets','roleRef','subjects',\
                        'rules','webhooks','secrets', 'data', 'metadata'] #'metadata' can be ignored 
    common_fields = set(state_node.keys()).intersection(set(important_fields)) 
    tmp = dict()
    for key in common_fields:
        tmp[key] = state_node[key]
    # build the prompt
    kind = state_node['kind']
    prompt = f"""
    The following JSON comes from a {kind} object. Focus on the 'spec' and 'status' fields
    (or other relevant fields if 'spec' and 'status' are not present) to find some clues for 
    the following error message, and ignore the resolution for this error.
    The error message is:\n{error_message} \n
    The JSON is:\n{tmp}
    """
    print(prompt)
   
   # add message and run assistant 
    semanticAnalyzer.add_message(prompt)
    print('run assistant')
    semanticAnalyzer.run_assistant()
   
    messages = semanticAnalyzer.wait_get_last_k_message(1)
    clue = messages.data[0].content[0].text.value
    
    return clue

