#!/usr/bin/env python

import os
import json
import csv
import re
from openpyxl import Workbook

def extract_simple_results(input_file):
    with open(input_file, 'r') as fi:
        raw_results = json.load(fi)
        simple_results = list()
        for raw_result in raw_results:
            print('+' * 100 + '\n')
            print(raw_result['error_message'] + '\n')
            print(raw_result['uuid'] + '\n')
            print('+' * 100 + '\n')
            simple_result = dict()
            simple_result['error_message'] = raw_result['error_message']
            simple_result['uuid'] = raw_result['uuid']
            simple_result['srckind'] = raw_result['srckind']
            simple_result['destkind'] = raw_result['destkind']
            simple_result['attempt'] = raw_result['attempt']

            #simple_result['locator_attempts']= raw_result['locator_attempts']
            analysis = raw_result['analysis']
            simple_result['metapaths'] = len(analysis)
            simple_result['analysis'] = list()
            for x in analysis:
                y = dict()
                if ('extend_metapath' in x):
                    y['extend_metapath'] = re.sub(r'\n\s+', '\n', x['extend_metapath']).replace('\n', '\n ')
                else:
                    y['extend_metapath'] = '' # for empty_metapath
                '''
                y['cypher_attempts'] = x['cypher_attempts']
                if 'human_cypher_query' in x:
                    y['human_cypher_attempts'] = 1
                else:
                    y['human_cypher_attempts'] = 0
                '''
                # for statepath, empty_statepath and empty_metapath
                y['statepath'] = list()
                if ('statepath' in x) and (len(x['statepath']) > 0):
                    for sp in x['statepath']:
                        #y['statepath'].append({'report': escape_and_loads(sp['report'])})
                        #y['statepath'].append({'report': sp['report']})
                        format_report(y['statepath'], sp)

                y['empty_statepath'] = list()
                if ('empty_statepath' in x) and (len(x['empty_statepath']) > 0):
                    for esp in x['empty_statepath']:
                        #y['empty_statepath'].append({'report': escape_and_loads(esp['report'])})
                        #y['empty_statepath'].append({'report': esp['report']})
                        format_report(y['empty_statepath'], esp)

                y['empty_metapath'] = list()
                if ('empty_metapath' in x) and (len(x['empty_metapath']) > 0):
                    for emp in x['empty_metapath']:
                        #y['empty_metapath'].append({'report': escape_and_loads(emp['report'])})
                        y['empty_metapath'].append({'report': emp['report']})

                simple_result['analysis'].append(y) 
            
            print(simple_result)
            simple_results.append(simple_result)
    return simple_results

def format_report(destlist, ele):
    if type(ele['report']) is dict:
        destlist.append({'report': ele['report']})
    elif type(ele['report']) is str:
        print('handle json string\n')
        destlist.append({'report': escape_and_loads(ele['report'])})
    else:
        print(f'type of element ({type(ele)}) is not supported.')

def escape_and_loads(raw_str):
    #'''
    if '```json' in raw_str:
        json_part = raw_str.split('```json')[1].split('```')[0].strip()
    elif '```JSON' in raw_str:
        json_part = raw_str.split('```JSON')[1].split('```')[0].strip()
    #elif '```' in raw_str:
    #    json_part = raw_str.split('```')[1].split('```')[0].strip()
    #else:
    #    json_part = raw_str
    #'''
    elif ('{' in raw_str) and ('}' in raw_str):
        first_brace_index = raw_str.find('{')
        last_brace_index = raw_str.rfind('}')
        json_part = raw_str[first_brace_index: last_brace_index + 1]
    else:
        json_part = raw_str

    escaped_str = json_part.replace("`", "'")\
                    .replace("\\\n", " ")\
                    .replace("\\n", " ")\
                    .replace("\n", " ")
    try:
        json_data = json.loads(escaped_str)
        print("JSON parsed successfully!")
        return json_data
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON: {e}")
        with open('./not-parsed-raw-string.log', 'a') as f:
            f.write(f"Failed to parse JSON: {e}\n")
            f.write(raw_str + '\n')
            f.write('~' * 100 + '\n')
            f.write(escaped_str + '\n')
            f.write('-' * 100 + '\n')
        return raw_str


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
    input_dir = './output-4-server-bracket/' # refine it if necessary
    output_dir = './output-4-server-simple-2/'

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
            
            # Add '-simple.json' to the file name
            json_filename = base_name + '-simple.json'
            os.makedirs(output_dir+'/json/', exist_ok=True)
            json_file_path = os.path.join(output_dir + '/json/', json_filename)
            print("New json full path:", json_file_path)
            save_json(simple_results, json_file_path)
            print('~' * 100)

            # Add '-simple.csv' to the file name
            csv_filename = base_name + '-simple.csv'
            os.makedirs(output_dir+'/csv/', exist_ok=True)
            csv_file_path = os.path.join(output_dir + '/csv/', csv_filename)
            print("New csv full path:", csv_file_path)
            save_csv(simple_results, csv_file_path)
            print('~' * 100)
           
            # Add '-simple.xlsx' to the file name
            xlsx_filename = base_name + '-simple.xlsx'
            os.makedirs(output_dir+'/xlsx/', exist_ok=True)
            xlsx_file_path = os.path.join(output_dir + '/xlsx/', xlsx_filename)
            print("New xlsx full path:", xlsx_file_path)
            save_xlsx(simple_results, xlsx_file_path)
            print('~' * 100)


if __name__ == "__main__":
    main()

