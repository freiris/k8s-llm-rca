#!/usr/bin/env/python

import os
import glob
import json
import csv
import numpy as np
import argparse


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
        
        # if human_label indicates True, we further check the resolution
        if metric == 'strict':
            false_list = ['<false>', '<none>']
        else:
            false_list = ['<false>']
        
        data_t = []
        for x in data:
            if x['human_label'].lower() not in false_list:
                data_t.append(x)
        
        data2 = []
        for x in data_t:
            if x['human_evaluation']['resolution_is_helpful'].lower() == '<false>':
                data2.append((x['uuid'], False))
            else:
                data2.append((x['uuid'], True))

        # count the helpful resoultion for correct RCA report
        count = 0
        for x in data2:
            if x[1]:
                count +=1

        total2 = len(data2)
        rate = count / total2

        print(f'helpful resolutions, total correct RCA report, rate: {count}, {total2}, {rate}')

        # write result to output-file 
        file_name = os.path.basename(input_file)
        xs = file_name.split('-')
        reason = xs[0]
        if (xs[1] == 'ExceedQuota') and (xs[2] in ['Job', 'ReplicaSet', 'StatefulSet']):
            msg_type = xs[1] + xs[2]
        else:
            msg_type = xs[1]
   
        header = ['count', 'total2', 'rate', 'file_name', 'reason', 'type']
        result = [count, total2, rate, file_name, reason, msg_type]

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

