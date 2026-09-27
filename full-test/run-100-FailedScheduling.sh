# for FailedScheduling-UnboundPVC
# 38 examples
#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedScheduling-UnboundPVC.csv -o ./output-1219-mini/FailedScheduling-UnboundPVC-out-1219-azure.json -b 0 -e 20

# example-20 will crash
#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedScheduling-UnboundPVC.csv -o ./output-1219-mini/FailedScheduling-UnboundPVC-out-1219-azure.json -b 28 -e 100

# run example-20 after we fix content-filtering bug
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/FailedScheduling-UnboundPVC.csv -o ./output-1219-mini/FailedScheduling-UnboundPVC-out-1219-azure.json -b 20 -e 100
