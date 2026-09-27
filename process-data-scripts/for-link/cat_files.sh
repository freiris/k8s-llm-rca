#!/bin/bash

# Directory containing the files
DIRECTORY=$1

# Navigate to the directory
cd "$DIRECTORY"

# Find and sort all files, then process them
find . -type f -name '*.json' |
awk -F'-[0-9]+\.json$' '{print $1}' | # Extract prefix
sort -u |                             # Get unique prefixes
while read PREFIX; do
    # Strip leading './' from PREFIX
    PREFIX=$(echo "$PREFIX" | sed 's|^\./||')
    
    # Create an output filename from the prefix
    OUTPUT_FILE="$PREFIX.json"

    # Concatenate the files with the same prefix into the output file
    cat "$PREFIX"-*.json > "$OUTPUT_FILE"

    echo "Combined files with prefix '$PREFIX' into '$OUTPUT_FILE'"
done
