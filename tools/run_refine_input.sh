#!/bin/bash

dir1="../INPUT/data-3/"
dir2="../INPUT/data-3-refine/"

for file in "$dir1"/*.csv; do
    base_name=$(basename "$file" .csv)
    output_file="$dir2/${base_name}-refined.csv"
    python3 refine_input.py -i "$file" -o "$output_file"
    
    #echo $file
    #echo $base_name
    #echo $output_file
done
