#!/usr/bin/env python

import os
import json
import csv
from openpyxl import Workbook

def extract_simple_results(input_file):
    with open(input_file, 'r') as fi:
        raw_results = json.load(fi)
        simple_results = list()
        for raw_result in raw_results:
            simple_result = dict()
            simple_result['error_message'] = raw_result['error_message']
            simple_result['uuid'] = raw_result['uuid']
            simple_result['attempt'] = raw_result['attempt']
            simple_result['locator_attempts']= raw_result['locator_attempts']
            simple_result['time_cost'] = raw_result['time_cost']
            simple_result['prompt_tokens'] = raw_result['token_usage']['prompt_tokens']
            simple_result['completion_tokens'] = raw_result['token_usage']['completion_tokens']
            simple_result['total_tokens'] = raw_result['token_usage']['total_tokens']
            simple_result['metapaths'] = len(raw_result['analysis']) 
            #simple_result['cypher_attempts'] = [x['cypher_attempts'] for x in raw_result['analysis']]
            simple_result['cypher_attempts'] = [(x['cypher_attempts'] if 'cypher_attempts' in x else 0) for x in raw_result['analysis']]
            simple_result['human_cypher_attempts'] = [(1 if 'human_cypher_query' in x else 0) for x in raw_result['analysis']]
            #simple_result['is_empty_statepath'] = [(1 if x['statepath'] == [] else 0) for x in raw_result['analysis']]
            simple_result['is_empty_statepath'] = [(1 if 'empty_statepath' in x else 0) for x in raw_result['analysis']]
            simple_result['is_empty_metapath'] = [(1 if 'empty_metapath' in x else 0) for x in raw_result['analysis']]
            
            # if the statepath is empty, we use human_cypher to double-check, so it is not a cypher failure, 
            # cypher_failures = cypher_attempts - 1
            # if the statepath is not empty, if we use humany_cypher, then all cypher_attempts failed

            simple_result['cypher_failures'] = list()
            for i, j, k, l in zip(simple_result['cypher_attempts'], simple_result['human_cypher_attempts'], simple_result['is_empty_statepath'], simple_result['is_empty_metapath']):
                if l > 0:
                    simple_result['cypher_failures'].append(0) # empty_metapath will not use cypher_query
                elif k > 0:
                    simple_result['cypher_failures'].append(i-1)
                elif j > 0:
                    simple_result['cypher_failures'].append(i)
                else:
                    simple_result['cypher_failures'].append(i-1)
            
            # for cypher_attempts
            simple_result['cypher_attempts_total'] = sum(simple_result['cypher_attempts'])
            simple_result['cypher_attempts_min'] = min(simple_result['cypher_attempts'])
            simple_result['cypher_attempts_max'] = max(simple_result['cypher_attempts'])
            simple_result['cypher_attempts_avg'] = simple_result['cypher_attempts_total']/len(simple_result['cypher_attempts'])
            
            # for cypher_failures
            simple_result['cypher_failures_total'] = sum(simple_result['cypher_failures'])
            simple_result['cypher_failures_min'] = min(simple_result['cypher_failures'])
            simple_result['cypher_failures_max'] = max(simple_result['cypher_failures'])
            simple_result['cypher_failures_avg'] = simple_result['cypher_failures_total']/len(simple_result['cypher_failures'])


            print(simple_result)
            simple_results.append(simple_result)
    return simple_results


def save_json(dict_list, json_file):
    with open(json_file, 'w') as fo:
        for dct in dict_list:
            json_record = json.dumps(dct, indent=4)
            fo.write(json_record + ',\n')
    print('finish write json')

def save_csv(dict_list, csv_file):
    # Open the file in write mode
    with open(csv_file, 'w', newline='') as f:
        # Create a DictWriter object with the keys of the dictionary as fieldnames parameter
        writer = csv.DictWriter(f, fieldnames=dict_list[0].keys())
        # Write header
        writer.writeheader()
        # Write the dictionary items to the CSV
        for dct in dict_list:
            writer.writerow(dct)
    print('finish write csv')

def to_excel_compatible(value):
    if isinstance(value, (dict, list, set)):
        return str(value)  # Convert complex types to string
    elif value is None:
        return ''  # Convert None to an empty string
    else:
        return value

def save_xlsx(rows, xlsx_file):
    # Create an Excel workbook and get the active sheet
    wb = Workbook()
    sheet = wb.active
    # Add the headers to the worksheet
    if rows:
        headers = rows[0].keys()
        sheet.append(list(headers))
        # Add the rows to the worksheet, converting values to Excel compatible format
        for row in rows:
            sheet.append([to_excel_compatible(row.get(header)) for header in headers])
    # Save the workbook to a file
    wb.save(xlsx_file)


def main():
    input_dir = './output-4-server-bracket/'
    output_dir = './output-4-server-statistic/'

    for filename in os.listdir(input_dir):
        if filename.startswith('.'):
            continue
        file_path = os.path.join(input_dir, filename)
        if os.path.isfile(file_path):
            print('%' * 100)
            print(f'input file is {file_path}')
            simple_results = extract_simple_results(file_path)
            
            print('~' * 100)
            # Remove the current file extension
            base_name = os.path.splitext(filename)[0]
            
            # Add '-stat.json' to the file name
            json_filename = base_name + '-stat.json'
            os.makedirs(output_dir+'/json/', exist_ok=True)
            json_file_path = os.path.join(output_dir + '/json/', json_filename)
            print("New json full path:", json_file_path)
            save_json(simple_results, json_file_path)
            print('~' * 100)

            # Add '-stat.csv' to the file name
            csv_filename = base_name + '-stat.csv'
            os.makedirs(output_dir+'/csv/', exist_ok=True)
            csv_file_path = os.path.join(output_dir + '/csv/', csv_filename)
            print("New csv full path:", csv_file_path)
            save_csv(simple_results, csv_file_path)
            print('~' * 100)
           
            # Add '-stat.xlsx' to the file name
            xlsx_filename = base_name + '-stat.xlsx'
            os.makedirs(output_dir+'/xlsx/', exist_ok=True)
            xlsx_file_path = os.path.join(output_dir + '/xlsx/', xlsx_filename)
            print("New xlsx full path:", xlsx_file_path)
            save_xlsx(simple_results, xlsx_file_path)
            print('~' * 100)


if __name__ == "__main__":
    main()

