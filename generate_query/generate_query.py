#!/usr/bin/env python

'''
import os
import openai
import time
import json
from openai import OpenAI
from neo4j import GraphDatabase

from neo4j_query_executor import Neo4jQueryExecutor
from openai_cypher_query_generator import OpenAICypherQueryGenerator
from openai_cypher_query_generator import build_generation_template
'''

from common.openai_generic_assistant import OpenAIGenericAssistant

def setup_cypher_generator():
    instructions="You are an expert in neo4j and cypher query language."
    name="cypher-query-generator"
    cypherQueryGenerator = OpenAIGenericAssistant()
    cypherQueryGenerator.create_assistant(instructions, name, 'gpt-4')
    cypherQueryGenerator.create_thread()
   
    #cypherQueryGenerator.retrieve_assistant(assistant_id="asst_m8xzRVAXj0feSEnkK4mMeuVs")
    #cypherQueryGenerator.retrieve_thread(thread_id="thread_yc9ndUIXbv3b5P3CeGXcvVsE")

    #cypherQueryGenerator.retrieve_assistant(assistant_id="asst_QED7U180yYfB1sBiOkGN9QCP")
    #cypherQueryGenerator.retrieve_thread(thread_id="thread_8g8SuEWW2aXnTbswJMSsUMms")

    print(cypherQueryGenerator.assistant.id)
    print(cypherQueryGenerator.thread.id)
    
    print(f'https://platform.openai.com/playground?assistant={cypherQueryGenerator.assistant.id}&thread={cypherQueryGenerator.thread.id}')


    label_message = "Let's label the following prompt template as generation-template-1, and use it to generate cypher query later"
    cypherQueryGenerator.add_message(label_message)

    generation_template = build_generation_template()
    cypherQueryGenerator.add_message(generation_template)
    
    return cypherQueryGenerator


def extend_metapath_construct_string(partial_path):
    nodes = partial_path.nodes
    relationships = partial_path.relationships
    srcKind = nodes[0]['kind']
    rels_str = f"""
    HasEvent, Event, EVENT, metadata_uid;
    ReferInternal, Event, {srcKind}, involvedObject_uid;
    """
    for rel in relationships:
        rel_str = (', ').join([rel.type, rel['srcKind'], rel['destKind'], rel['key']])
        rels_str += rel_str + ';\n'
    return rels_str

# Note: metapath is a string here
def generate_cypher_query(metapath_str, error_message, cypherQueryGenerator):
    # build the prompt 
    prompt = f"""
    Let's use generation-template-1 and generate a cypher query for the following example. Strictly follow the (srcKind)-[rel]->(destkind) ordering, don't reverse it. Return the generated query in the following format:
    ```cypher
    generated_cypher_query
    ```
    the provided metapath is:
    {metapath_str}
    the error message to filtering is:
    {error_message}
    """
    cypherQueryGenerator.add_message(prompt)

    print('run assistant')
    cypherQueryGenerator.run_assistant()
    messages = cypherQueryGenerator.wait_get_last_k_message(1)
    cypher_query = extract_cypher(messages.data[0].content[0].text.value)
    
    print('the generated cypher query is :\n %s' % cypher_query)

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
        print('Warning: ALL records are not message compatible')
    
    return res

def message_compatible(record):
    #message = record[2]['message']
    for ele in record:
        if ele['kind'] == 'Event':
            message = ele['message'] 
    # by default, dest is the last element
    dest = record[len(record)-1]
    # check if the name is in the message
    if dest['isNative'] == 'true':
        k1 = 'name2'
    elif dest['isAtomic'] == 'true':
        k1 = 'val'
    elif dest['tag'] in ['nfs', 'hostPath']:
        k1 = 'path'
    elif dest['tag'] == 'container':
        k1 = 'containerName'
    elif dest['tag'] == 'image':
        k1 = 'imageName'
   
    # check if the kind is in the message
    if dest['isNative'] == 'true':
        k2 = 'kind2'
    elif dest['isNative'] == 'false':
        k2 = 'tag'

    return (dest[k1] in message) or (dest[k2] in message)
   



def build_generation_template():
    template = """
    Cypher Query Generation Prompt Template
Use this template to construct a Cypher query that follows a specific metapath and filters 'EVENT' nodes based on the content of a 'message' property. As an example, we'll use a case where a 'ConfigMap' is not found.
    1. Analyze the Metapath and Error Message:
        ○ Break down the metapath into its components, where each segment includes a relationship type (relType), source node type (srcKind), destination node type (destKind), and a characteristic value (propertyValue) associated with a consistently named property on the relationship. This property is uniformly named 'key' across relationships. To filter for a specific relationship, you reference this 'key' along with the provided characteristic value, as expressed in the pattern r.key = 'propertyValue'.
        ○ Identify the error message to be used for filtering, paying attention to its exact wording for string matching.

    2. Start with Filtering EVENT Nodes:
        ○ Begin by matching EVENT nodes that have a property named 'message'.
        ○ Use a WHERE clause with the CONTAINS function to tolerate variations like trailing spaces or word case in the message
        ○ Ensure the full error message is included in the query's WHERE clause against the 'message' property of the EVENT nodes, without truncation.
        ○ Apply a LIMIT to narrow down the results early:
        MATCH (evt:EVENT)
        WHERE evt.message CONTAINS 'Your error message here'
        WITH evt
        LIMIT 1
        
    3. Chain MATCH Clauses Based on the Metapath:
        ○ Continue the query by adding MATCH clauses for each part of the provided metapath. For each segment of the metapath, use the node type (srcKind and destKind) as the label for the source and destination node. Use the relationship type (relType) as the label for the connecting edge, and apply a WHERE clause based on the 'key' property value (propertyValue) specified for that relationship:
   
        MATCH (startNode:srcKind)-[r1:relType]->(node1:destKind)
        WHERE r1.key = 'propertyValue'
   
        ○ For consecutive relationships, increment the relationship alias sequentially to use unique identifiers such as r1, r2, r3, etc. This ensures clarity when multiple relationships are present in the MATCH pattern:

        MATCH (node1:srcKind)-[r2:relType]->(node2:destKind)
        WHERE r2.key = 'propertyValue'

        ... and so on for additional relationships.
        
        ○ Ensure to use the same node alias for each node type, particularly if that node type appears in multiple relationships to maintain consistency. For example:

        MATCH (evt: EVENT),
        MATCH (n1:Event)-[r1:HasState]->(evt: EVENT),
        MATCH (n1:Event)-[r2:ReferInternal]->(n2: Pod)

    4. Adhere Strictly to the Provided Labels and Property Values:
        ○ Use the node and relationship labels exactly as provided in the metapath without adjustments or reinterpretations.  For instance, if the label given is 'nfs', it should not be changed to 'NFS' or any other variation.
        ○ Ensure correct case sensitivity and spelling to match the labels in your Neo4j database exactly.
        ○ Use the property value exactly as provided in the metapath without adjustments or reinterpretations. For instance, if the property value is 'involvedObject_uid', don't omit the '_' or use other variation.

    5. Timely Filtering:
        ○ Apply the filters as soon as possible after each MATCH clause, rather than aggregating all filtering at the end of the query.
        ○ Timely filtering helps to reduce the search space and improve query performance.

    6. Construct the RETURN Statement:
        ○ Include all the matched nodes and relationships in the RETURN clause to generate the complete path as specified by the metapath:

RETURN startNode, rel, destNode, …

    7. Example Based on a ConfigMap Not Found Case:

    Provided Metapath:
    HasEvent, Event, EVENT, metadata_uid;
    ReferInternal, Event, Pod, involvedObject_uid;
    ReferInternal, Pod, ConfigMap, spec_volumes_configMap_name

    Error Message for Filtering:
    MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found

    Generated Cypher Query:
    
    MATCH (evt:EVENT)
    WHERE evt.message CONTAINS 'MountVolume.SetUp failed for volume "gen-white-list-conf" : configmap "es-gen-white-list-configmap" not found'
    WITH evt
    LIMIT 1
    MATCH (event:Event)-[r1:HasEvent]->(evt)
    WHERE r1.key = 'metadata_uid'
    MATCH (event)-[r2:ReferInternal]->(pod:Pod)
    WHERE r2.key = 'involvedObject_uid'
    MATCH (pod)-[r3:ReferInternal]->(configMap:ConfigMap)
    WHERE r3.key = 'spec_volumes_configMap_name'
    RETURN event, r1, evt, r2, pod, r3, configMap

    """

    return template


def human_generate_cypher_query(metapath_str, error_message):
    # Parse the metapath string into list
    mp = metapath_str.split(';')[:-1]
    metapath = [rel.strip().split(', ') for rel in mp]
    
    # Start with Filtering EVENT Nodes
    query_parts = []
    node_aliases = {"EVENT": "evt"}

    query_parts.append(f"""
MATCH (evt:EVENT)
WHERE evt.message CONTAINS {repr(error_message)}
WITH evt
LIMIT 1""")

    # Build node alias for each srcKind and destKind
    idx = 1
    for rel in metapath:
        srcKind = rel[1]
        destKind = rel[2]
        if srcKind not in node_aliases:
            node_aliases[srcKind] = f"n{idx}"
            idx += 1
        if destKind not in node_aliases:
            node_aliases[destKind] = f"n{idx}"
            idx += 1

    # Chain MATCH Clauses Based on the Metapath with Timely Filtering
    for idx, (relType, srcKind, destKind, propertyValue) in enumerate(metapath, start=1):
        srcAlias = node_aliases[srcKind]
        destAlias = node_aliases[destKind]

        query_parts.append(f"""
MATCH ({srcAlias}:{srcKind})-[r{idx}:{relType}]->({destAlias}:{destKind})
WHERE r{idx}.key = {repr(propertyValue)}""")

    # Construct the RETURN Statement
    nodes = list(node_aliases.values())
    rels = [f"r{idx}" for idx in range(1, len(metapath)+1)]
    assert len(nodes) == len(rels) + 1

    return_vars = [None]*(len(nodes)+len(rels))
    return_vars[::2] = nodes
    return_vars[1::2] = rels

    query_parts.append(f"""
RETURN {', '.join(return_vars)}""")

    # Combine all parts into a complete Cypher query
    complete_query = '\n'.join(query_parts) # or '\n'.join() with new line
    
    print(f'the human generated cypher query is: \n{complete_query}')
    return complete_query.strip()


