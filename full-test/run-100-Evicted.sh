# for Evicted-LowOnResource-repeat-refined-10
# 21 examples, not clearly related to dependence, test split-run
python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Evicted-LowOnResource-repeat.csv -o ./output-4/Evicted-LowOnResource-repeat-out-0811-azure.json -b 0 -e 10

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Evicted-LowOnResource-repeat.csv -o ./output-4/Evicted-LowOnResource-repeat-out-0811-azure.json -b 10 -e 100 


# for Evicted-NodeDiskPressure-repeat-refined-10
# 24 examples, not clearly related to dependence, test split-run
#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Evicted-NodeDiskPressure-repeat.csv -o ./output-4/Evicted-NodeDiskPressure-repeat-out-0811-azure.json -b 0 -e 10

#python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Evicted-NodeDiskPressure-repeat.csv -o ./output-4/Evicted-NodeDiskPressure-repeat-out-0811-azure.json -b 10 -e 100

python3 ../test_with_file_args_extend_index_azure.py -i ./data-4/Evicted-NodeDiskPressure-repeat.csv -o ./output-4/Evicted-NodeDiskPressure-repeat-out-0811-azure.json -b 18 -e 100 
