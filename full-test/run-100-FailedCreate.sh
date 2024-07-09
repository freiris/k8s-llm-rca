# for FailedCreate-ExceedQuota-Job
python3 ../test_with_file_args_extend_index.py -i ./data-4/FailedCreate-ExceedQuota-Job.csv -o ./output-4/FailedCreate-ExceedQuota-Job-out-0703.json -b 3 -e 100

# for FailedCreate-ExceedQuota-ReplicaSet
python3 ../test_with_file_args_extend_index.py -i ./data-4/FailedCreate-ExceedQuota-ReplicaSet.csv -o ./output-4/FailedCreate-ExceedQuota-ReplicaSet-out-0703.json -b 3 -e 100

# for FailedCreate-ExceedQuota-StatefulSet
python3 ../test_with_file_args_extend_index.py -i ./data-4/FailedCreate-ExceedQuota-StatefulSet-repeat.csv -o ./output-4/FailedCreate-ExceedQuota-StatefulSet-repeat-out-0703.json -b 3 -e 100

# for FailedCreate-ServiceAccountNotFound
python3 ../test_with_file_args_extend_index.py -i ./data-4/FailedCreate-ServiceAccountNotFound.csv -o ./output-4/FailedCreate-ServiceAccountNotFound-out-0703.json -b 3 -e 100

