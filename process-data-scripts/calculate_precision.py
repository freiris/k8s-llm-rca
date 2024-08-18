#!/usr/bin/env/python

import os
import glob
import json
import csv
import numpy as np
import argparse


def get_acc_value(xs):
    if len(xs) == 1:
        return [xs[0], xs[0], xs[0]]
    elif len(xs) == 2:
        return [xs[0], (xs[0] or xs[1]), (xs[0] or xs[1])]
    elif len(xs) >= 3:
        return [xs[0], (xs[0] or xs[1]), (xs[0] or xs[1] or xs[2])]

# Function to process all files in a directory
def run(input_directory, result_file):
    json_files = glob.glob(os.path.join(input_directory, '*.json'))

    for i, json_file in enumerate(json_files):
        mode = 'w' if i == 0 else 'a'  # Write the header only once when in 'w' mode initially
        print(f'mode = {mode}')
        print(json_file)
        process_file(json_file, result_file, mode)

    print(f"Processed {len(json_files)} files and consolidated into {result_file}")


def process_file(input_file, output_file, mode):
    # import json 
    with open(input_file, 'r') as fi:
        data = json.load(fi)
        
        # only '<False>' convert to False
        data2 = [ (x['uuid'], False if x['human_label'].lower() == '<false>' else True) for  x in data]
         
        # merge consecutive identical uuid, due to attempt>1
        data3 = [(data2[0][0], [data2[0][1]])]
        for x in data2[1:]:
            # append to the last one if same, otherwise, start a new one
            if x[0] == data3[-1][0]:
                data3[-1][1].append(x[1])
            else:
                data3.append((x[0], [x[1]]))

        # get accumulate True/False 
        data4 = [(x[0], get_acc_value(x[1])) for x in data3]
        
        # count the True for acc1, acc2 and acc3
        # acc3 is the precision which means eventually correct ratio 
        count1 = 0
        count2 = 0
        count3 = 0
        for x in data4:
            if x[1][0]:
                count1 += 1
            if x[1][1]:
                count2 += 1
            if x[1][2]:
                count3 += 1

        total = len(data4)
        print(f'for acc1~acc3: {count1}, {count2}, {count3}, {total}')      
        print(f'acc1 = {count1}/{total}')
        # count all Ture 
        count = 0
        for x in data2:
            if x[1]:
                count += 1

        total2 = len(data2)
        print(f'for overall attempts: {count}, {total2}')

        acc1 = count1 *1.0 / total
        acc2 = count2 / total
        acc3 = count3 / total
        ratio = count / total2
        
        # write result to output-file 
        file_name = os.path.basename(input_file)
        xs = file_name.split('-')
        reason = xs[0]
        if (xs[1] == 'ExceedQuota') and (xs[2] in ['Job', 'ReplicaSet', 'StatefulSet']):
            msg_type = xs[1] + xs[2]
        else:
            msg_type = xs[1]
    

        header = ['count1', 'count2', 'count3', 'total', 'acc1', 'acc2', 'acc3', 'count', 'total2', 'ratio', 'file_name', 'reason', 'type']
        result = [count1, count2, count3, total, acc1, acc2, acc3, count, total2, ratio, file_name, reason, msg_type]
        with open(output_file, mode) as fo:
            writer = csv.writer(fo)
            if mode == 'w':
                writer.writerows([header])
            else:
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


    # Parse arguments
    args = parser.parse_args()

    # Pass the command line arguments to main function
    run(args.input_directory, args.output_file)

