#!/bin/bash

# Function to add '-bracket' suffix if not present
add_bracket_suffix() {
    local dir=$1
    if [[ $dir != *"-bracket" ]]; then
        echo "${dir}-bracket"
    else
        echo $dir
    fi
}

# Check if directory is provided as argument
if [ -z "$1" ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

original_dir=$1

# Check if the original directory exists
if [ ! -d "$original_dir" ]; then
    echo "Directory '$original_dir' does not exist."
    exit 1
fi

# Determine the new directory name
new_dir=$(add_bracket_suffix "$original_dir")

# Create the new directory if it doesn't exist
if [ ! -d "$new_dir" ]; then
    mkdir "$new_dir"
fi

# Copy contents from the original directory to the new directory
cp -r "$original_dir/"* "$new_dir/"

echo "Contents of '$original_dir' have been copied to '$new_dir'."

# Loop through each file in the directory
for file in $new_dir/*.json; do
    # Echo processing  
    echo "processing '$file'"
	
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
done

echo "Complete add square brackets to each json file."
