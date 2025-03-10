import pandas as pd
import numpy as np
import json
import os
import time
import datetime
import warnings
import re
import requests
from sqlalchemy import create_engine,text

warnings.filterwarnings("ignore")

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
            df_id = pd.read_csv(file_path, on_bad_lines='skip')  # Skip bad lines



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
    for id_entry in merged_hid_list :
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
    """Merges suffixes split by `/` in the hid string."""
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

# ✅ Fetch mould data
def fetch_mould_data():
    df_id = pd.DataFrame()
    dataframes = []

    # ✅ Get today's date dynamically
    today_date = datetime.datetime.today().strftime('%Y-%m-%d')

    # ✅ Read SQL queries and replace static date
    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_queries = file.read()
    
    sql_queries = sql_queries.replace("'2025-03-07'", f"'{today_date}'")

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
                        print(df.head())  
                        dataframes.append(df)
            except Exception as e:
                print(f"❌ SQL Execution Error: {e}")

    df_id = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

    # ✅ Update reference ID only if data exists
    if not df_id.empty and "ProductionDate" in df_id and "StartTime" in df_id:
        production_date = df_id["ProductionDate"].max()
        start_time = df_id["StartTime"].max()
        print(f"🔄 Updated Reference: Production Date = {production_date}, Start Time = {start_time}")
    else:
        print("⚠️ No data available, skipping reference update.")

    return df_id
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


df_id['Pattern number'] = df_id['PatternIdentification'].apply(lambda x: get_max_pattern(x, df_hid['hid'].tolist()))



df_id=df_id[config["columns_required"]]

df_id.to_json("df_pattern.json", orient="records", indent=4)

print("Data saved successfully in JSON format: df_pattern.json")
df_id.to_excel("df_pattern_2.xlsx",index=False)



#  Process data
def process_data():
    df_id = fetch_mould_data()

    if df_id.empty:
        print(" No new data available. Skipping JSON update.")
        return

    df_id["componentId"] = df_id["PatternIdentification"].apply(get_max_pattern)
    df_id = df_id.dropna(subset=["componentId"])
    df_id["componentId"] = df_id["componentId"].astype(int)
    df_id["TotalPourStatus"] = df_id["TotalPourStatus"].astype(int)

    # Convert `ProductionDate` to `yyyy-mm-dd`
    df_id["date"] = pd.to_datetime(df_id["ProductionDate"]).dt.strftime('%Y-%m-%d')

    # Format time correctly
    df_id["startTime"] = df_id["StartTime"].apply(lambda x: str(x).split()[-1] if isinstance(x, pd.Timedelta) else str(x))
    df_id["endTime"] = df_id["EndTime"].apply(lambda x: str(x).split()[-1] if isinstance(x, pd.Timedelta) else str(x))

    #  Map values
    df_id["noOfBoxesPoured"] = df_id["TotalPourStatus"].where(df_id["TotalPourStatus"].notna(), None)
    df_id["totalMould"] = df_id["TotalPourStatus"].where(df_id["TotalPourStatus"].notna(), None)
    df_id["unpouredMould"] = None  
    df_id["foundryLine"] = df_id.apply(lambda x: {"pkey": 1}, axis=1)
    df_id["badBatches"] = None
    df_id["noOfBatches"] = None

    df_id.rename(columns={"Shift": "shift"}, inplace=True)

    #  Filter required columns
    df_id = df_id[config["columns_required"]]


    #  Save to JSON
    json_file_path = "data.json"
    df_id.to_json(json_file_path, orient="records", indent=4)

    print(f" Data formatted and saved successfully in {json_file_path}")

    #  Send data to API
    send_data_to_api(json_file_path)

#  Send data to API
def send_data_to_api(json_file_path):
    try:
        with open(json_file_path, "r", encoding="utf-8") as json_file:
            data_text = json_file.read()
    except FileNotFoundError:
        print(f" Error: {json_file_path} not found!")
        return

    #  Login to API
    session = requests.Session()
    login_payload = {"j_username": USERNAME, "j_password": PASSWORD}
    login_response = session.post(LOGIN_URL, data=login_payload)

    if login_response.status_code == 200:
        print(" Login successful!")
    else:
        print(f" Login failed! Status Code: {login_response.status_code}, Response: {login_response.text}")
        return

    #  Extract JSESSIONID
    jsessionid = session.cookies.get("JSESSIONID")
    if not jsessionid:
        print(" Failed to retrieve JSESSIONID.")
        return

    print(f" Using JSESSIONID: {jsessionid}")

    #  Send POST request
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"JSESSIONID={jsessionid}"
    }

    response = requests.post(API_URL, data=data_text, headers=headers)

    #  Print API response
    print(" Response Status Code:", response.status_code)
    print("Response Body:", response.text)

#  Run the process every `data_frequency` minutes
while True:
    try:
        process_data()
    except Exception as e:
        print(f"Unexpected error: {e}")
    time.sleep(data_frequency) 