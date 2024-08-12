# for FailedCreate-ExceedQuota-Job
# 45 examples, require split-run
#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-Job.csv -o ./output-4/FailedCreate-ExceedQuota-Job-out-0811-azure.json -b 0 -e 15

#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-Job.csv -o ./output-4/FailedCreate-ExceedQuota-Job-out-0811-azure.json -b 15 -e 30

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-Job.csv -o ./output-4/FailedCreate-ExceedQuota-Job-out-0811-azure.json -b 30 -e 100

# for FailedCreate-ExceedQuota-ReplicaSet
# 32 examples, require split-run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-ReplicaSet.csv -o ./output-4/FailedCreate-ExceedQuota-ReplicaSet-out-0811-azure.json -b 0 -e 10

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-ReplicaSet.csv -o ./output-4/FailedCreate-ExceedQuota-ReplicaSet-out-0811-azure.json -b 10 -e 20

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-ReplicaSet.csv -o ./output-4/FailedCreate-ExceedQuota-ReplicaSet-out-0811-azure.json -b 20 -e 100

# for FailedCreate-ExceedQuota-StatefulSet
# 20 examples, split-run may help
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-StatefulSet-repeat.csv -o ./output-4/FailedCreate-ExceedQuota-StatefulSet-repeat-out-0811-azure.json -b 0 -e 10

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ExceedQuota-StatefulSet-repeat.csv -o ./output-4/FailedCreate-ExceedQuota-StatefulSet-repeat-out-0811-azure.json -b 10 -e 100

# for FailedCreate-ServiceAccountNotFound
# 32 examples, previous result is good in a single run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedCreate-ServiceAccountNotFound.csv -o ./output-4/FailedCreate-ServiceAccountNotFound-out-0811-azure.json -b 0 -e 100

