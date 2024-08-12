# for Failed-AccessDenied-not
# 38 examples, test split-run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Failed-AccessDenied.csv -o ./output-4/Failed-AccessDenied-out-0811-azure.json -b 0 -e 20 

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Failed-AccessDenied.csv -o ./output-4/Failed-AccessDenied-out-0811-azure.json -b 20 -e 100 


# for Failed-ArtifactNotFound
# 19 examples, previous result is good in a single run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Failed-ArtifactNotFound-repeat.csv -o ./output-4/Failed-ArtifactNotFound-repeat-out-0811-azure.json -b 0 -e 100

# for Failed-NetworkUnreachable
# 20 examples, test split-run 
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Failed-NetworkUnreachable-repeat.csv -o ./output-4/Failed-NetworkUnreachable-repeat-out-0811-azure.json -b 0 -e 10

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Failed-NetworkUnreachable-repeat.csv -o ./output-4/Failed-NetworkUnreachable-repeat-out-0811-azure.json -b 10 -e 100


# for Failed-NoVolumeToMount
# 23 examples, intrinsic difficult, test split-run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Failed-NoVolumeToMount-repeat.csv -o ./output-4/Failed-NoVolumeToMount-repeat-out-0811-azure.json -b 0 -e 10

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Failed-NoVolumeToMount-repeat.csv -o ./output-4/Failed-NoVolumeToMount-repeat-out-0811-azure.json -b 10 -e 100

