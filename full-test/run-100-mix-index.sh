#!/bin/bash

# Check if the correct number of arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <low_index> <high_index>"
    exit 1
fi

# Assign command-line arguments to variables
low_index=$1
high_index=$2

# Ensure the output directory exists
output_dir="./output-4-mix"
mkdir -p "$output_dir"

# Loop through the range from low_index to high_index and run the Python script
for i in $(seq "$low_index" "$high_index"); do
    input_file="./data-4-mix/Split_Result_${i}.csv"
    output_file="${output_dir}/Split_Result_${i}_out-0708.json"
    
    # Run the Python script with the input and output files
    python3 ../test_with_file_args_extend_index.py -i "$input_file" -o "$output_file" -b 0 -e 100
done
