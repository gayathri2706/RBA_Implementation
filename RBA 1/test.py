import pandas as pd
import numpy as np
from datetime import timedelta
import warnings
warnings.filterwarnings("ignore")
import os
import re
import json


cd=os.getcwd()
data_dir=os.path.join(cd,"data")
config_dir=os.path.join(cd,"config")

for file_name in os.listdir(config_dir):
        if file_name.endswith('.json'):
            file_path = os.path.join(config_dir, file_name)
            with open(file_path, "r") as config_file:
                config = json.load(config_file)

for file in os.listdir(data_dir):
     if file.startswith("Pattern") and file.endswith(".xlsx"):
             file_path = os.path.join(data_dir, file)
             df_hid = pd.read_excel(file_path,skiprows=5)
     elif file.startswith("mold") and file.endswith(".csv"):
            file_path = os.path.join(data_dir, file)
            df_id = pd.read_csv(file_path, error_bad_lines=False)



def remove_numeric_suffix(hid):
    """Removes the last numeric part from HID for matching, but keeps the full HID for pattern extraction."""
    match = re.search(r'^(.*?)-\d+[A-Za-z]*$', hid.strip())
    return match.group(1) if match else hid

def search_with_suffix(id_list, hid_list):
    """Step 1: Match with Numeric Suffix and Component Suffix."""
    pattern_numbers = []
    for id_entry in id_list:
        for hid in hid_list:
            if id_entry in hid:
                try:
                    pattern_numbers.append(int(hid.split('-')[-1]))
                except ValueError:
                    pass
    return max(pattern_numbers) if pattern_numbers else None

def direct_search(id_list, hid_list):
    """Step 2: Directly search ID in HID List."""
    pattern_numbers = []
    merged_hid_list = [merge_suffixes(hid) for hid in id_list]
    for id_entry in merged_hid_list :
        for hid in hid_list:
            if id_entry in hid:
                try:
                    pattern_numbers.append(int(hid.split('-')[-1]))
                except ValueError:
                    pass
    return max(pattern_numbers) if pattern_numbers else None

def numeric_search(id_list, hid_list):
    """Step 3: Search only numeric part of ID in HID List."""
    pattern_numbers = []
    for id_entry in id_list:
        numeric_part = ''.join(filter(str.isdigit, id_entry))
      
        for hid in hid_list:
            cleaned_hid= remove_numeric_suffix(hid)
            if numeric_part in cleaned_hid:
                try:
                    pattern_numbers.append(int(hid.split('-')[-1]))
                except ValueError:
                    pass
    return max(pattern_numbers) if pattern_numbers else None

def get_component_suffixes(component_string):
    """Map component names to their respective suffixes and return all possible suffixes."""
    component_suffix_map =config['component_map']
    components = [c.strip().upper() for c in component_string.split(',')]
    suffixes = [suffix for c in components if c in component_suffix_map for suffix in component_suffix_map[c]]
    return suffixes

def merge_suffixes(hid):
    """Merges suffixes split by `/` in the HID string."""
    return re.sub(r'(\w+-\d+)([A-Za-z])/([A-Za-z])', r'\1\2\3', hid)

def remove_last_suffix(hid_list):
    cleaned_hids = []
    for hid in hid_list:
        cleaned_hid = re.sub(r'-\d+[A-Za-z]*$', '', hid)
        cleaned_hids.append(cleaned_hid)
    return cleaned_hids
def extract_base_id(identification_list, hid_list):
    pattern_numbers = []
    
    for identification in identification_list:
        match = re.search(r'\b([A-Za-z]*\d+)\b', identification)
        if match:
            data = match.group(1)
            
            for hid in hid_list:
                if re.fullmatch(rf'{re.escape(data)}-\d+[A-Za-z]*', hid):
                    try:
                        pattern_numbers.append(int(hid.split('-')[-1])) 
                    except ValueError:
                        pass

    return max(pattern_numbers) if pattern_numbers else None



def extract_number_with_suffix(id_list, hid_list, suffix_list):
    """Extract the first full number before and after '-', then combine with suffixes."""
    pattern_numbers = []

    for identification in id_list:
        match = re.search(r'(\d+)-(\d+)', identification)
        if match:
            first_number = match.group(1) 

            for suffix in suffix_list:  
                extracted_value = first_number +  suffix  

                for hid in hid_list:
                    if extracted_value in hid:
                        try:
                            pattern_numbers.append(int(hid.split('-')[-1]))  
                        except ValueError:
                            pass

    return max(pattern_numbers) if pattern_numbers else None



def get_max_pattern(identification, hid_list):
    parts = identification.split('/#')
    component_string = parts[0].strip()
    component_suffixes = get_component_suffixes(component_string)
    id_list = parts[-1].split(',')
    id_list = [''.join(re.sub(r'\s+', '', item)) for item in id_list]
    merged_hid_list = [merge_suffixes(hid) for hid in id_list]
    pattern_numbers = []

    for suffix in component_suffixes:
        pattern_number = search_with_suffix([id_entry + suffix for id_entry in merged_hid_list], hid_list)
        if pattern_number:
            pattern_numbers.append(pattern_number)

    if not pattern_numbers:
        pattern_number = direct_search(id_list, hid_list)
        if pattern_number:
            pattern_numbers.append(pattern_number)
    if not pattern_numbers:
        pattern_number = numeric_search(id_list, hid_list)
        if pattern_number:
            pattern_numbers.append(pattern_number)
    if not pattern_numbers:
        pattern_number = extract_base_id(id_list, hid_list)
        if pattern_number:
            pattern_numbers.append(pattern_number)
    if not pattern_numbers:
        pattern_number = extract_number_with_suffix(id_list, hid_list,component_suffixes)
        if pattern_number: 
            pattern_numbers.append(pattern_number)
    
    if pattern_numbers: 
            return max(pattern_numbers)
    print(f"Error: The pattern number is not found for identification: {identification}")
    return None


df_id['Pattern number'] = df_id['PatternIdentification'].apply(lambda x: get_max_pattern(x, df_hid['Hid'].tolist()))



df_id=df_id[config["columns_required"]]

df_id.to_json("df_pattern.json", orient="records", indent=4)

print("Data saved successfully in JSON format: df_pattern.json")
df_id.to_excel("df_pattern_2.xlsx",index=False)

