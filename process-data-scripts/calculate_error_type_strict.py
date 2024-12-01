#!/usr/bin/env/python

import os
import glob
import json
import csv
import numpy as np
import argparse

'''
def determine_retry(analysis):
    # there can be multiple metapaths for each error_message,
    # and each metapath can correspond to one/more statepath (or empty_statepath),
    # we only retry if ALL the metapaths and ALL statepaths/empty_statepaths can not explain the error_message
    # tips: empty_metapath does not have 'further_investigation', but we retry it by default
    for aly in analysis:
        if 'statepath' in aly:
            for x in aly['statepath']:
                if str(x['report']['further_investigation']).lower() == 'false':
                    return False
        elif 'empty_statepath' in aly:
            for x in aly['empty_statepath']:
                if str(x['report']['further_investigation']).lower() == 'false':
                    return False
    return True
'''

# gpt-4 simplify the code
def determine_retry_simple(analysis):
    for aly in analysis:
        paths = aly.get('statepath', []) + aly.get('empty_statepath', []) # empty_metapath has not 'further_investigation' key
        if any(str(x['report']['further_investigation']).lower() == 'false' for x in paths):
            return False
    return True




# Function to process all files in a directory
def run(input_directory, result_file, metric):
    json_files = glob.glob(os.path.join(input_directory, '*.json'))

    for i, json_file in enumerate(json_files):
        mode = 'w' if i == 0 else 'a'  # Write the header only once when in 'w' mode initially
        print(f'mode = {mode}')
        print(json_file)
        process_file(json_file, result_file, metric, mode)
        print('-' * 100 + '\n')

    print(f"Processed {len(json_files)} files and consolidated into {result_file}")


def process_file(input_file, output_file, metric, mode):
    # import json 
    with open(input_file, 'r') as fi:
        data = json.load(fi)
        
        # for human_label, '<False>' and '<none>' are treated as False
        if metric == 'strict':
            print('Use strict metric, view None as False')
            data2 = [ (x['uuid'], False if x['human_label'].lower() in ['<false>', '<none>'] else True) for  x in data]
        else:
            print('Use loose metric, view None as True')
            data2 = [ (x['uuid'], False if x['human_label'].lower() in ['<false>'] else True) for  x in data]
        
        # for further-investigation, we reuse the function in k8s-llm-rac-azure, 
        # if further-investigation is ture, then llm thinks current analysis is not good enough
        data3 = [(x['uuid'], not determine_retry_simple(x['analysis'])) for x in data]
            
        total2 = len(data)
        false_positive = 0
        false_negative = 0
        true_positive = 0
        true_negative = 0
        for i in range(total2):
            actual = data2[i][1]
            claim = data3[i][1]
            if actual == True and claim == False:
                false_negative += 1
            elif actual == True and claim == True:
                true_positive += 1
            elif actual == False and claim == False:
                true_negative += 1
            elif actual == False and claim == True:
                false_positive += 1
        
        fnr = false_negative / total2
        tpr = true_positive / total2
        tnr = true_negative / total2
        fpr = false_positive / total2

        print(f'for error types: fp = {false_positive}, fn = {false_positive}, tp = {true_positive}, tn = {true_negative}, total2 = {total2}, fpr = {fpr}, fnr = {fnr}, tpr = {tpr}, tnr = {tnr}')

        # write result to output-file 
        file_name = os.path.basename(input_file)
        xs = file_name.split('-')
        reason = xs[0]
        if (xs[1] == 'ExceedQuota') and (xs[2] in ['Job', 'ReplicaSet', 'StatefulSet']):
            msg_type = xs[1] + xs[2]
        else:
            msg_type = xs[1]
    

        header = ['false_positive', 'false_negative', 'true_positive', 'true_negative', 'total2', 'fpr', 'fnr', 'tpr', 'tnr', 'file_name', 'reason', 'type']
        result = [false_positive, false_negative, true_positive, true_negative, total2, fpr, fnr, tpr, tpr, file_name, reason, msg_type]
        with open(output_file, mode) as fo:
            writer = csv.writer(fo)
            if mode == 'w':
                writer.writerows([header])
            # write result for both 'w' and 'a' mode    
            writer.writerows([result]) 


if __name__ == "__main__":
    # Initialize parser
    parser = argparse.ArgumentParser(
        description="Process input directory and output a csv_file."
    )

    # Add arguments for input and output file paths
    parser.add_argument(
        '-i', '--input-directory',
        type=str,
        required=True,
        help='Path to the input directory with json files'
    )
    parser.add_argument(
        '-o', '--output-file',
        type=str,
        required=True,
        help='Path to the output file'
    )
    # Add argument for the metric type (strict or loose)
    parser.add_argument(
    '-m', '--metric',
    type=str,
    choices=['strict', 'loose'],
    default='strict',
    help='Choose the metric type: "strict" or "loose". Default is "strict".'
    )


    # Parse arguments
    args = parser.parse_args()

    # Pass the command line arguments to main function
    run(args.input_directory, args.output_file, args.metric)

