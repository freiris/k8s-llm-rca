#!/bin/bash

dir1="../INPUT/data-3/"
dir2="../INPUT/data-3-local/"

for file in "$dir1"/*.csv; do
    base_name=$(basename "$file" .csv)
    local_file="$dir2/${base_name}.csv"
    
    echo $file
    #echo $base_name
    echo $local_file
    diff $file $local_file
    printf '+%.0s' {1..100}; echo
done
