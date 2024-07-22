#!/bin/bash

# Navigate to the directory containing the files
cd ./output-4-server-simple-2-bracket

# Loop through each file in the directory
for file in *; do
    if [ -f "$file" ]; then
        # Create a temporary file to store the changes
        tmpfile=$(mktemp)
        
        # Write the first line
        echo '[{' > "$tmpfile"
        
        # Skip the first line and append all other lines from the original file except the last line to the temp file
        sed '1d;$d' "$file" >> "$tmpfile"
        
        # Write the last line
        echo '}]' >> "$tmpfile"
        
        # Move the temporary file to replace the original file
        mv "$tmpfile" "$file"
    fi
done
