#!/usr/bin/bash 


# Evicted-LowOnResource-repeat.csv 
python3 test_with_file_args.py -i ./data-2-argument/Evicted-LowOnResource-repeat.csv  -o ./output-3/Evicted-LowOnResource-repeat-new-result.json

#Evicted-NodeDiskPressure-repeat.csv
python3 test_with_file_args.py -i ./data-2-argument/Evicted-NodeDiskPressure-repeat.csv  -o ./output-3/Evicted-NodeDiskPressure-repeat-new-result.json

#Failed-AccessDenied-repeat.csv
python3 test_with_file_args.py -i ./data-2-argument/Failed-AccessDenied-repeat.csv  -o ./output-3/Failed-AccessDenied-repeat-new-result.json

#Failed-ArtifactNotFound-repeat.csv
python3 test_with_file_args.py -i ./data-2-argument/Failed-ArtifactNotFound-repeat.csv  -o ./output-3/Failed-ArtifactNotFound-repeat-new-result.json

#Failed-NetworkUnreachable.csv  # complete

#Failed-NoVolumeToMount-repeat.csv
python3 test_with_file_args.py -i ./data-2-argument/Failed-NoVolumeToMount-repeat.csv  -o ./output-3/Failed-NoVolumeToMount-repeat-new-result.json

#FailedCreate-ExceedQuota-StatefulSet.csv # complete

#FailedMount-ObjectNotRegistered-repeat.csv
python3 test_with_file_args.py -i ./data-2-argument/FailedMount-ObjectNotRegistered-repeat.csv  -o ./output-3/FailedMount-ObjectNotRegistered-repeat-new-result.json

#FailedMount-StaleNFS.csv
python3 test_with_file_args.py -i ./data-2-argument/FailedMount-StaleNFS.csv  -o ./output-3/FailedMount-StaleNFS-new-result.json


# FailedCreate-ExceedQuota-Job-pick.csv
python3 test_with_file_args.py -i ./data-2-argument/FailedCreate-ExceedQuota-Job-pick.csv  -o ./output-3/FailedCreate-ExceedQuota-Job-pick-new-result.json


# FailedCreate-ExceedQuota-ReplicaSet-pick.csv
python3 test_with_file_args.py -i ./data-2-argument/FailedCreate-ExceedQuota-ReplicaSet-pick.csv  -o ./output-3/FailedCreate-ExceedQuota-ReplicaSet-pick-new-result.json

# FailedMount-FailedSyncConfigMapCache-pick.csv
python3 test_with_file_args.py -i ./data-2-argument/FailedMount-FailedSyncConfigMapCache-pick.csv  -o ./output-3/FailedMount-FailedSyncConfigMapCache-pick-new-result.json

# FailedMount-NoSuchFileDir-pick.csv
python3 test_with_file_args.py -i ./data-2-argument/FailedMount-NoSuchFileDir-pick.csv  -o ./output-3/FailedMount-NoSuchFileDir-pick-new-result.json

