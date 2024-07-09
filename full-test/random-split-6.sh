#!/bin/bash

# Input and output directories
input_dir="./data-4-copy" # use a copy
output_dir="./data-4-mix"

# Ensure output directory exists
mkdir -p "$output_dir"

# Temporary files to hold headers and content
header_file="headers.txt"
content_file="content.txt"
tmp_file="tmp_content.txt"

# Ensure temporary files are empty
> "$header_file"
> "$content_file"
> "$tmp_file"

# Step 1: Convert line endings of input files to Unix-style
for file in "$input_dir"/*.csv; do
    dos2unix "$file"
done

# Extract headers and store them separately
for file in "$input_dir"/*.csv; do
    head -n 1 "$file" >> "$header_file"  # Extract header
    
    # Add a newline at the end of each file's content to ensure separation
    tail -n +2 "$file" | sed '$a\' >> "$content_file"
done

# Remove duplicate headers from the header file
sort -u "$header_file" > "$header_file.tmp" && mv "$header_file.tmp" "$header_file"

# Step 2: Determine the longest header
longest_header=$(awk ' { if (length > x) { x = length; y = $0 } }END{ print y }' "$header_file")

# Step 3: Shuffle the content using `shuf`
shuf "$content_file" > "$tmp_file"

# Step 4: Split the shuffled content into 10 files
total_lines=$(wc -l < "$tmp_file")
split_size=$((total_lines / 10))
remainder=$((total_lines % 10))

split -l "$split_size" "$tmp_file" tmp_split_

# Step 5: Add the longest header to each split file and handle the remainder if necessary
i=1
for split_file in tmp_split_*; do
    echo "$longest_header" > "$output_dir/Split_Result_${i}.csv"
    cat "$split_file" >> "$output_dir/Split_Result_${i}.csv"
    
    if [ "$i" -eq 10 ]; then
        # Ensure remainder lines go into the last file
        if [ $remainder -ne 0 ]; then
            tail -n "$remainder" "$tmp_file" >> "$output_dir/Split_Result_${i}.csv"
        fi
        break
    fi

    i=$((i + 1))
done

# Clean up temporary files
rm "$header_file" "$content_file" "$tmp_file" tmp_split_*
