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

#  Load configuration file
for file_name in os.listdir(config_dir):
    if file_name.endswith('.json'):
        file_path = os.path.join(config_dir, file_name)
        with open(file_path, "r") as config_file:
            config = json.load(config_file)

#  Extract database and API credentials
db_config = config["database"]
# pat_config = config["pat_database"]
sql_file_path = config["sql_file_path"]
data_frequency = config["data_frequency"]



API_URL = config["api_url"]
LOGIN_URL = config["login_url"]
USERNAME = config["j_username"]
PASSWORD = config["j_password"]

#  Establish database connection
engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database_name']}")

# #  Establish database connection for pattern master
# engine = create_engine(f"mysql+pymysql://{pat_config['user']}:{pat_config['password']}@{pat_config['host']}:{pat_config['port']}/{pat_config['database_name']}")


#  Fetch pattern components
query = "SELECT date, pattern_no, pattern_name, hid FROM rba_data.pattern_component;"
df_hid = pd.read_sql(query, engine)

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
    """Step 2: Directly search ID in HID List with exact match after removing the last suffix."""
    pattern_numbers = []
    merged_hid_list = [merge_suffixes(hid) for hid in id_list]

    for id_entry in merged_hid_list:
        for hid in hid_list:
            base_hid = "-".join(hid.split('-')[:-1])  # Remove the last suffix
            if id_entry == base_hid:  # Exact match after removing suffix
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
    if any('-' in item for item in id_list):
         id_list = [''.join(re.sub(r'\s+', '', item)) for item in id_list]
    else:
         id_list = [re.sub(r'\s+', '-', item) for item in id_list]
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
    # if not pattern_numbers:
    #     pattern_number = numeric_search(id_list, hid_list)
    #     if pattern_number:
    #         pattern_numbers.append(pattern_number)
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


#  Fetch mould data
def fetch_mould_data():
    today_date = datetime.datetime.today().strftime('%Y-%m-%d')

    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_queries = file.read()

    #sql_queries = sql_queries.replace("'2025-03-07'", f"'{today_date}'")
    sql_queries = sql_queries.replace("TimePour >= '2025-03-12 07:00:00'", f"TimePour >= '{today_date} 07:00:00'")
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
                        print(f" Query Successful: {len(df)} rows fetched")
                        dataframes.append(df)
            except Exception as e:
                print(f" SQL Execution Error: {e}")

    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

def clean_json_data(df):
    df = df.copy()

    # ✅ Ensure `date` is a numeric timestamp before conversion
    if "date" in df.columns:
        df["date"] = df["date"].apply(lambda x: datetime.datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d') 
                                      if isinstance(x, (int, float)) else x)

    # ✅ Convert `componentId` to integer if possible
    if "componentId" in df.columns:
        df["componentId"] = df["componentId"].apply(lambda x: int(x) if str(x).isdigit() else None)

    # ✅ Replace `null` values with `""` (Empty string)
    df = df.where(pd.notna(df), None)
    df = df.replace({None: ""})  # Convert NaN to empty string for API

    return df.to_dict(orient="records")  # ✅ Return as a dictionary, not JSON string

def send_data_to_api(json_data):
    try:
        # ✅ Debug: Print JSON before sending
        print("📤 JSON Payload Being Sent:\n", json.dumps(json_data, indent=4))

        # ✅ Login to API
        session = requests.Session()
        login_payload = {"j_username": USERNAME, "j_password": PASSWORD}
        login_response = session.post(LOGIN_URL, data=login_payload)

        if login_response.status_code == 200:
            print("✅ Login successful!")

            # ✅ Extract cookies
            cookies = session.cookies.get_dict()
            print("🍪 Cookies received:", cookies)

            # ✅ Send POST request with correctly formatted JSON
            headers = {
                "Content-Type": "application/json",
                "Cookie": f"JSESSIONID={cookies.get('JSESSIONID', '')}"
            }

            # ✅ Send JSON properly
            response = session.post(API_URL, json=json_data, headers=headers)  # ✅ FIXED

            # ✅ Print API response
            print("📩 Response Status Code:", response.status_code)
            print("📩 Response Body:", response.text)
        else:
            print(f"❌ Login failed! Status Code: {login_response.status_code}, Response: {login_response.text}")
            return
    except Exception as e:
        print(f"❌ Unexpected error: {e}")  # ✅ Print error details

#  Process data
def process_data():
    df_id = fetch_mould_data()
    if df_id.empty:
        print("❌ No new data available. Skipping JSON update.")
        return

    # ✅ Apply pattern search
    missing_patterns = set()
    df_id = df_id[~df_id['PatternIdentification'].isna()]
    df_id['componentId'] = df_id['PatternIdentification'].apply(lambda x: get_max_pattern(x, df_hid['hid'].tolist()))

    # ✅ Drop NaN before conversion
    df_id.dropna(subset=["componentId"], inplace=True)
    df_id["componentId"] = df_id["componentId"].astype(int)

    for index, row in df_id.iterrows():
        if pd.isna(row["componentId"]):
            missing_patterns.add(f"Error: The pattern number is not found for identification: {row['PatternIdentification']}")

    # ✅ Log missing pattern numbers
    for error in sorted(missing_patterns):
        print(error)

    print(df_id)  # ✅ Debugging step

    df_id["TotalPourStatus"] = df_id["TotalPourStatus"].astype(int)

    # ✅ Ensure `ProductionDate` exists before conversion
    if "ProductionDate" in df_id.columns:
        df_id["date"] = pd.to_datetime(df_id["ProductionDate"]).dt.strftime('%Y-%m-%d')
    else:
        print("❌ ERROR: 'ProductionDate' column is missing from DataFrame!")
        return  # Stop execution if missing

    # ✅ Convert `StartTime` and `EndTime` to Timedelta before extracting time
    df_id["StartTime"] = pd.to_timedelta(df_id["StartTime"], errors='coerce')
    df_id["EndTime"] = pd.to_timedelta(df_id["EndTime"], errors='coerce')

    # ✅ Extract only the `HH:MM:SS` part, removing "0 days"
    df_id["startTime"] = df_id["StartTime"].apply(lambda x: str(x).split()[-1] if pd.notna(x) else None)
    df_id["endTime"] = df_id["EndTime"].apply(lambda x: str(x).split()[-1] if pd.notna(x) else None)


    # ✅ Map values
    df_id["noOfBoxesPoured"] = df_id["TotalPourStatus"].where(df_id["TotalPourStatus"].notna(), None)
    df_id["totalMould"] = df_id["TotalPourStatus"].where(df_id["TotalPourStatus"].notna(), None)
    df_id["unpouredMould"] = None  
    df_id["foundryLine"] = df_id.apply(lambda x: {"pkey": 1}, axis=1)
    df_id["badBatches"] = None
    df_id["noOfBatches"] = None
    df_id.rename(columns={"Shift": "shift"}, inplace=True)

    # ✅ Ensure only required columns are kept
    df_id = df_id[config["columns_required"]]

    # ✅ Clean JSON Data
    json_data = clean_json_data(df_id)
    print(json.dumps(json_data, indent=4))  # ✅ Print final JSON for debugging
    print("✅ Data formatted successfully.")

    # ✅ Send data to API
    send_data_to_api(json_data)

    # ✅ Print reference update
    last_row = df_id.iloc[-1] if not df_id.empty else None
    if last_row is not None:
        print(f"✅ Updated Reference: Production Date = {last_row['date']}, Start Time = {last_row['startTime']}")

#  Run the process every `data_frequency` minutes
while True:
    try:
        process_data()
    except Exception as e:
        print(f" Unexpected error: {e}")
    time.sleep(data_frequency)
