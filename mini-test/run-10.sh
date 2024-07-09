#!/bin/bash

# Define directories
input_dir="./input-head-10"
output_dir="./out-head-10"

# make dir if not exist
mkdir -p $output_dir

# Loop through each CSV file in the input directory
for file in "$input_dir"/*.csv; do
    # Extract the base name of the file
    base_name=$(basename "$file" .csv)
    
    # Create the output file path
    output_file="${output_dir}/${base_name}-out.json"
   
    # Run python script 
    echo $file
    echo $output_file
    python3 ../test_with_file_args_extend_index.py -i "$file" -o "$output_file" -b 0 -e 3

done
