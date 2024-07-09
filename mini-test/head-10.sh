#!/bin/bash

# Define directories
input_dir="../INPUT/data-3-refine"
output_dir="./"

# Loop through each CSV file in the input directory
for file in "$input_dir"/*.csv; do
    # Extract the base name of the file
    base_name=$(basename "$file" .csv)
    
    # Create the output file path
    output_file="${output_dir}/${base_name}-10.csv"
    
    # Use head to get the first 10 lines and output to the new file
    head -n 10 "$file" > "$output_file"
done
