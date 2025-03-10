import pandas as pd
import numpy as np
import json
import os
import time
import datetime
import warnings
import re
import requests
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

cd = os.getcwd()
data_dir = os.path.join(cd, "data")
config_dir = os.path.join(cd, "config")

# ✅ Load configuration file
for file_name in os.listdir(config_dir):
    if file_name.endswith('.json'):
        file_path = os.path.join(config_dir, file_name)
        with open(file_path, "r") as config_file:
            config = json.load(config_file)

# ✅ Extract database and API credentials
db_config = config["database"]
sql_file_path = config["sql_file_path"]
data_frequency = config["data_frequency"]

DB_USER = db_config["user"]
DB_PASSWORD = db_config["password"]
DB_HOST = db_config["host"]
DB_PORT = db_config["port"]
DB_NAME = db_config["database_name"]

API_URL = config["api_url"]
LOGIN_URL = config["login_url"]
USERNAME = config["j_username"]
PASSWORD = config["j_password"]

# ✅ Establish database connection
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# ✅ Fetch pattern components
query = "SELECT date, pattern_no, pattern_name, hid FROM rba_data.pattern_component;"
df_hid = pd.read_sql(query, engine)

# ✅ Helper Functions
def remove_numeric_suffix(hid):
    """Removes the last numeric part from hid for matching, but keeps the full hid for pattern extraction."""
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
    """Step 2: Directly search ID in hid List."""
    pattern_numbers = []
    merged_hid_list = [merge_suffixes(hid) for hid in id_list]
    for id_entry in merged_hid_list:
        for hid in hid_list:
            if id_entry in hid:
                try:
                    pattern_numbers.append(int(hid.split('-')[-1]))
                except ValueError:
                    pass
    return max(pattern_numbers) if pattern_numbers else None

def numeric_search(id_list, hid_list):
    """Step 3: Search only numeric part of ID in hid List."""
    pattern_numbers = []
    for id_entry in id_list:
        numeric_part = ''.join(filter(str.isdigit, id_entry))
        for hid in hid_list:
            cleaned_hid = remove_numeric_suffix(hid)
            if numeric_part in cleaned_hid:
                try:
                    pattern_numbers.append(int(hid.split('-')[-1]))
                except ValueError:
                    pass
    return max(pattern_numbers) if pattern_numbers else None

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
                extracted_value = first_number + suffix
                for hid in hid_list:
                    if extracted_value in hid:
                        try:
                            pattern_numbers.append(int(hid.split('-')[-1]))
                        except ValueError:
                            pass
    return max(pattern_numbers) if pattern_numbers else None

def get_component_suffixes(component_string):
    """Map component names to their respective suffixes and return all possible suffixes."""
    component_suffix_map = config['component_map']
    components = [c.strip().upper() for c in component_string.split(',')]
    suffixes = [suffix for c in components if c in component_suffix_map for suffix in component_suffix_map[c]]
    return suffixes

def merge_suffixes(hid):
    """Merges suffixes split by `/` in the hid string."""
    return re.sub(r'(\w+-\d+)([A-Za-z])/([A-Za-z])', r'\1\2\3', hid)

# ✅ Fetch mould data
def fetch_mould_data():
    today_date = datetime.datetime.today().strftime('%Y-%m-%d')
    
    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_queries = file.read()

    sql_queries = sql_queries.replace("'2025-03-07'", f"'{today_date}'")
    dataframes = []

    with engine.connect() as connection:
        for statement in sql_queries.split(";"):
            statement = statement.strip()
            if not statement:
                continue
            try:
                result = connection.execute(text(statement))
                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    if not df.empty:
                        print(f"✅ Query Successful: {len(df)} rows fetched")
                        dataframes.append(df)
            except Exception as e:
                print(f"❌ SQL Execution Error: {e}")

    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

# ✅ Process data
def process_data():
    df_id = fetch_mould_data()
    if df_id.empty:
        print("⚠️ No new data available. Skipping JSON update.")
        return

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
            pattern_number = extract_number_with_suffix(id_list, hid_list, component_suffixes)
            if pattern_number:
                pattern_numbers.append(pattern_number)

        return max(pattern_numbers) if pattern_numbers else None

    df_id["Pattern number"] = df_id["PatternIdentification"].apply(lambda x: get_max_pattern(x, df_hid['hid'].tolist()))
    df_id = df_id[config["columns_required"]]
    
    df_id.to_json("data.json", orient="records", indent=4)
    print("✅ Data formatted and saved successfully.")

# ✅ Run the process every `data_frequency` minutes
while True:
    try:
        process_data()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    time.sleep(data_frequency)
