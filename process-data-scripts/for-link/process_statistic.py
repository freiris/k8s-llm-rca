#!/usr/bin/env python

import os
import glob
import csv
import numpy as np
import argparse

def calculate_avg_std(values):
    # Convert the list to a numpy array
    arr = np.array(values)
    
    # Calculate the average (mean)
    avg = np.mean(arr)
    
    # Calculate the standard deviation
    std = np.std(arr)
    
    return avg, std

# Function to process all files in a directory
def run(input_directory, result_file):
    csv_files = glob.glob(os.path.join(input_directory, '*.csv'))
    
    for i, csv_file in enumerate(csv_files):
        mode = 'w' if i == 0 else 'a'  # Write the header only once when in 'w' mode initially
        print(f'mode = {mode}')
        print(csv_file)
        process_file(csv_file, result_file, mode)
            
    print(f"Processed {len(csv_files)} files and consolidated into {result_file}")


def process_file(input_file, output_file, mode):
    # read in csv file 
    rows = []
    with open(input_file, 'r') as fi:
        reader = csv.reader(fi)
        header = next(reader)
        for x in reader:
            rows.append(x)
    
    # ['error_message', 'uuid', 'attempt', 'locator_attempts', 'time_cost', 'prompt_tokens', 'completion_tokens', 'total_tokens', 'metapaths', 'cypher_attempts', 'human_cypher_attempts', 'is_empty_statepath', 'is_empty_metapath', 'cypher_failures', 'cypher_attempts_total', 'cypher_attempts_min', 'cypher_attempts_max', 'cypher_attempts_avg', 'cypher_failures_total', 'cypher_failures_min', 'cypher_failures_max', 'cypher_failures_avg']
        
    # note: 'attempt' is the order of trial, 1st, 2nd or 3rd
    # 'uuid' and 'message' can be repeated, we calculate the number of example processed by count 'attempt=1'
    idx = header.index('attempt')
    attempt = [int(x[idx]) for x in rows]
    attempts = len(attempt)
    examples = 0
    for x in attempt:
        if x == 1:
            examples += 1

    # 'locator_attempts' sum
    idx = header.index('locator_attempts')
    locator_attempts = sum([int(x[idx]) for x in rows]) 

    # 'metapaths' sum
    idx = header.index('metapaths') 
    metapaths = sum([int(x[idx]) for x in rows])
   
    # 'cypher_attempts_total' sum
    idx = header.index('cypher_attempts_total')
    cypher_attempts = sum([int(x[idx]) for x in rows])

    # 'cypher_failures_total' sum 
    idx = header.index('cypher_failures_total')
    cypher_failures = sum([int(x[idx]) for x in rows]) 
    
    print('examples, attempts, metapaths, cypher_attempts, cypher_failures')
    print(f'{examples}, {attempts}, {metapaths}, {cypher_attempts}, {cypher_failures}')
    
    # calculate avg, std for 'time_cost', 'prompt_tokens', 'completion_tokens', 'total_tokens'
    # time_cost
    idx = header.index('time_cost')
    time_cost = [float(x[idx]) for x in rows]  
    time_cost_avg, time_cost_std = calculate_avg_std(time_cost) 
    
    print(f'time_cost avg+std: {time_cost_avg}, {time_cost_std}')
    
    # prompt_tokens
    idx = header.index('prompt_tokens')
    prompt_tokens = [float(x[idx]) for x in rows]
    prompt_tokens_avg, prompt_tokens_std = calculate_avg_std(prompt_tokens)

    print(f'prompt_tokens avg+std: {prompt_tokens_avg}, {prompt_tokens_std}')

    # completion_tokens
    idx = header.index('completion_tokens')
    completion_tokens = [float(x[idx]) for x in rows]
    completion_tokens_avg, completion_tokens_std = calculate_avg_std(completion_tokens)

    print(f'completion_tokens avg+std: {completion_tokens_avg}, {completion_tokens_std}')

    # total_tokens
    idx = header.index('total_tokens')
    total_tokens = [float(x[idx]) for x in rows]
    total_tokens_avg, total_tokens_std = calculate_avg_std(total_tokens)

    print(f'total_tokens avg+std: {total_tokens_avg}, {total_tokens_std}')
 
    # write to output_file, put the input_filename at the end
    file_name = os.path.basename(input_file)

    xs = file_name.split('-')
    reason = xs[0]
    if (xs[1] == 'ExceedQuota') and (xs[2] in ['Job', 'ReplicaSet', 'StatefulSet']):
        msg_type = xs[1] + xs[2]
    else:
        msg_type = xs[1]

    output_header = ['examples', 'attempts', 'locator_attempts', 'metapaths', 'cypher_attempts', 'cypher_failures',\
                    'time_cost_avg', 'time_cost_std', 'prompt_tokens_avg', 'prompt_tokens_std',
                    'completion_tokens_avg', 'completion_tokens_std', 'total_tokens_avg', 'total_tokens_std',\
                    'file_name', 'reason', 'type']
    
    result = [examples, attempts, locator_attempts, metapaths, cypher_attempts, cypher_failures,\
                    time_cost_avg, time_cost_std, prompt_tokens_avg, prompt_tokens_std,\
                    completion_tokens_avg, completion_tokens_std, total_tokens_avg, total_tokens_std,\
                    file_name, reason, msg_type]
    
    # only write the output_header for the 'w' mode, i.e, first file
    with open(output_file, mode) as fw:
        writer = csv.writer(fw)
        print(f'mode in process_file is {mode}')
        if mode == 'w':
            writer.writerows([output_header])
        else:
            writer.writerows([result])
          

if __name__ == "__main__":
    # Initialize parser
    parser = argparse.ArgumentParser(
        description="Process input directory and output a file."
    )

    # Add arguments for input and output file paths
    parser.add_argument(
        '-i', '--input-directory',
        type=str,
        required=True,
        help='Path to the input directory with csv files'
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

