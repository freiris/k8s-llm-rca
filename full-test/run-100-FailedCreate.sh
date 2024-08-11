# for FailedCreate-ExceedQuota-Job

#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-Job.csv -o ./output-4/FailedCreate-ExceedQuota-Job-out-0722.json -b 0 -e 3

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-Job.csv -o ./output-4/FailedCreate-ExceedQuota-Job-out-0810-debug-azure.json -b 1 -e 3

# for FailedCreate-ExceedQuota-ReplicaSet
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-ReplicaSet.csv -o ./output-4/FailedCreate-ExceedQuota-ReplicaSet-out-0810-debug-azure.json -b 0 -e 3

# for FailedCreate-ExceedQuota-StatefulSet
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-StatefulSet-repeat.csv -o ./output-4/FailedCreate-ExceedQuota-StatefulSet-repeat-out-0810-debug-azure.json -b 0 -e 3 

# for FailedCreate-ServiceAccountNotFound
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ServiceAccountNotFound.csv -o ./output-4/FailedCreate-ServiceAccountNotFound-out-0810-debug-azure.json -b 0 -e 3

