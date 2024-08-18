# for FailedMount-ConfigMapNotFound
# 43 examples, previous result is good in a single run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-ConfigMapNotFound.csv -o ./output-4/FailedMount-ConfigMapNotFound-out-0811-azure.json -b 0 -e 100

# for FailedMount-FailedSyncConfigMapCache
# 56 examples, previous result is bad, require split-run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncConfigMapCache.csv -o ./output-4/FailedMount-FailedSyncConfigMapCache-out-0811-azure.json -b 0 -e 15

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncConfigMapCache.csv -o ./output-4/FailedMount-FailedSyncConfigMapCache-out-0811-azure.json -b 15 -e 30

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncConfigMapCache.csv -o ./output-4/FailedMount-FailedSyncConfigMapCache-out-0811-azure.json -b 30 -e 45

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncConfigMapCache.csv -o ./output-4/FailedMount-FailedSyncConfigMapCache-out-0811-azure.json -b 45 -e 100


# for FailedMount-FailedSyncSecretCache
# 57 examples, previous result is bad, require split-run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncSecretCache.csv -o ./output-4/FailedMount-FailedSyncSecretCache-out-0811-azure.json -b 0 -e 15

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncSecretCache.csv -o ./output-4/FailedMount-FailedSyncSecretCache-out-0811-azure.json -b 15 -e 30

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncSecretCache.csv -o ./output-4/FailedMount-FailedSyncSecretCache-out-0811-azure.json -b 30 -e 45

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-FailedSyncSecretCache.csv -o ./output-4/FailedMount-FailedSyncSecretCache-out-0811-azure.json -b 45 -e 100


#'''
# for FailedMount-NoSuchFileDir
# 47 examples, and the 3rd example will crash, split-run may help 
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-NoSuchFileDir.csv -o ./output-4/FailedMount-NoSuchFileDir-out-0811-azure.json -b 0 -e 2

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-NoSuchFileDir.csv -o ./output-4/FailedMount-NoSuchFileDir-out-0811-azure.json -b 3 -e 15

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-NoSuchFileDir.csv -o ./output-4/FailedMount-NoSuchFileDir-out-0811-azure.json -b 15 -e 30

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-NoSuchFileDir.csv -o ./output-4/FailedMount-NoSuchFileDir-out-0811-azure.json -b 30 -e 100
#'''


# for FailedMount-ObjectNotRegistered
# 24 examples, previous result is good in a single run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-ObjectNotRegistered-repeat.csv -o ./output-4/FailedMount-ObjectNotRegistered-repeat-out-0811-azure.json -b 0 -e 100

# for FailedMount-SecretNotFound
# 56 examples, previous result turn worse, require split-run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-SecretNotFound.csv -o ./output-4/FailedMount-SecretNotFound-out-0811-azure.json -b 0 -e 20

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-SecretNotFound.csv -o ./output-4/FailedMount-SecretNotFound-out-0811-azure.json -b 20 -e 40

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-SecretNotFound.csv -o ./output-4/FailedMount-SecretNotFound-out-0811-azure.json -b 40 -e 100

#'''
# for FailedMount-StaleNFS
# 21 examples, previous result is good, we still use split-run 
#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-StaleNFS-repeat.csv -o ./output-4/FailedMount-StaleNFS-repeat-out-0811-azure.json -b 0 -e 3

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-StaleNFS-repeat.csv -o ./output-4/FailedMount-StaleNFS-repeat-out-0811-azure.json -b 3 -e 10

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedMount-StaleNFS-repeat.csv -o ./output-4/FailedMount-StaleNFS-repeat-out-0811-azure.json -b 10 -e 100
#'''
