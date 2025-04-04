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

LAST_SENT_START_TIME = None  # 🔁 Initialize this global variable


API_URL = config["api_url"]
LOGIN_URL = config["login_url"]
USERNAME = config["j_username"]
PASSWORD = config["j_password"]

#  Establish database connection
engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database_name']}")

# #  Establish database connection for pattern master
# engine = create_engine(f"mysql+pymysql://{pat_config['user']}:{pat_config['password']}@{pat_config['host']}:{pat_config['port']}/{pat_config['database_name']}")


def fetch_pattern_components():
    query = "SELECT date, pattern_no, pattern_name, hid FROM rba_data.pattern_component;"
    
    connection = engine.connect()  # Open connection explicitly
    try:
        df_hid = pd.read_sql(query, connection)
       
    finally:
        connection.close()  # ✅ Close connection explicitly
    
    return df_hid


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
    return pattern_numbers if pattern_numbers else None

def direct_search(id_list, hid_list):
    """Step 2: Directly search ID in hid List with exact match after removing the last suffix."""
    pattern_numbers = []
    merged_hid_list = [merge_suffixes(hid) for hid in id_list]

    for id_entry in merged_hid_list:
        for hid in hid_list:
            base_hid = "-".join(hid.split('-')[:-1]) 
            if id_entry == base_hid:  
                try:
                    pattern_numbers.append(int(hid.split('-')[-1]))
                except ValueError:
                    pass

    return pattern_numbers if pattern_numbers else None

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
    return pattern_numbers if pattern_numbers else None

def get_component_suffixes(component_string, config):
    """Map component names to their respective suffixes and return all possible suffixes."""
    component_suffix_map = config['component_map']
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

    return pattern_numbers if pattern_numbers else None

def extract_number_with_suffix(id_list, hid_list, suffix_list):
    """Extract all matching pattern numbers instead of only the max one."""
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

    return pattern_numbers if pattern_numbers else None

def count_pattern_occurrences(hid_list):
    """Count how many times each pattern appears in the hid list."""
    pattern_count = Counter(remove_numeric_suffix(hid) for hid in hid_list)
    return pattern_count


def split_identification(identification):
    # Handle cases with and without brackets
    match = re.match(r"(.+?)/#([\w-]+)\s*\(([^)]+)\)", identification)
    if match:
        main_part, bracket_part, extra_part = match.groups()
        return main_part.strip(), bracket_part.strip(), extra_part.strip(),""
    else:
        parts = identification.split('/#') 
        parts_1=parts[1].split(',')#
        parts_1.append(parts[0])
        parts_1 = [parts_1[-1]] + parts_1[:-1]
        if '&' in parts[0]:
                parts = parts[0].split('&') + [parts[1]]
                return parts[0].strip(), parts[1].strip(), parts[2].strip(),""
        elif len(parts_1) > 2:
            if len(parts_1)>=2:
                return parts_1[0].strip(), parts_1[1].strip(), parts_1[2].strip(),""
            else:
                return parts_1[0].strip(), parts_1[1].strip(), parts_1[2].strip(),parts_1[3].strip()
        elif len(parts) == 2:
            if ',' in parts[1]:
                parts_1 = parts[1].split(',')  # Create a list by splitting parts[1]
                parts_1.append(parts[0])
                parts_1 = [parts_1[-1]] + parts_1[:-1]
                return parts_1[0].strip(), parts_1[1].strip(), parts_1[2].strip(),""
            else:
                return parts[0].strip(), parts[1].strip(), "",""
        return identification.strip(), "", ""
    
def extract_base_identifier(identifier):
    """Extracts only the relevant base part of an identifier and removes leading/trailing spaces."""
    identifier = identifier.strip()  # Remove extra spaces
    match = re.match(r'([A-Za-z0-9]+(?:-[A-Za-z0-9]+)*)', identifier)
    return match.group(1) if match else identifier

def get_best_pattern(identification, hid_list, df_hid,config):
    #parts = identification.split('/#')
    #component_string = parts[0].strip()
    #print(identification)

    match = df_hid[df_hid['hid'] == identification]  # Filter rows where 'hid' matches
    if not match.empty:
        return match['pattern_no'].values[0]  # Return the first matching 'Pattern No'
    
    elif '/#' in identification:   
        component_string, id_string, extra_info,_ = split_identification(identification)
        component_suffixes = get_component_suffixes(component_string,config)
        if extra_info:
            id_list = extra_info.split(',')
        else:
            id_list = id_string.split(',')

        if any('-' in item for item in id_list):
            id_list = [''.join(re.sub(r'\s+', '', item)) for item in id_list]
        else:
            id_list = [re.sub(r'\s+', '-', item) for item in id_list]

        merged_hid_list = [merge_suffixes(hid) for hid in id_list]
        pattern_numbers = []

        # Step 1: Try matching with suffixes
        for suffix in component_suffixes:
            pattern_number = search_with_suffix([id_entry + suffix for id_entry in merged_hid_list], hid_list)
            if pattern_number:
                pattern_numbers.append(pattern_number)

        if not pattern_numbers:
            pattern_number = direct_search(id_list, hid_list)
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

        if not pattern_numbers:
            component_string, id_string, extra_info,_ = split_identification(identification)
            id_list = id_string.split(',')
        
            matching_rows = df_hid[df_hid['hid'].str.contains('|'.join(id_list), na=False, case=False)]

            if not matching_rows.empty:
                # Convert 'PatternNo' column values to a list of integers
                pattern_numbers = matching_rows['pattern_no'].dropna().astype(int).unique().tolist()  
        
        

        if not pattern_numbers:
            matching_rows = df_hid[df_hid['pattern_name'].str.contains('|'.join(id_list), na=False, case=False)]
            if not matching_rows.empty:
                pattern_numbers = matching_rows['pattern_no'].unique().tolist()

        if not pattern_numbers:
            parts = identification.split('/#')
            numbers = re.findall(r'\d+', parts[1])
            numbers = ''.join(numbers)  # Convert list to a string
            if component_suffixes:
                id_list = [numbers + suffix for suffix in component_suffixes]  # Concatenate with suffixes

                matching_rows = df_hid[df_hid['hid'].str.contains('|'.join(id_list), na=False, case=False)]
                
                if not matching_rows.empty:
                    # Convert 'PatternNo' column values to a list of integers
                    pattern_numbers = matching_rows['pattern_no'].dropna().astype(int).unique().tolist()
        if pattern_numbers:
        # Flatten pattern_numbers if needed
            pattern_numbers = [num for sublist in pattern_numbers for num in sublist] if any(isinstance(i, list) for i in pattern_numbers) else pattern_numbers

            # Count occurrences in df_hid
            pattern_counts = df_hid['pattern_no'].value_counts().to_dict()

            # Get the count for each matched pattern number
            pattern_info = [(num, pattern_counts.get(num, 0)) for num in pattern_numbers]

            pattern_1 = list({num for num, count in pattern_info if count == 1})
            pattern_2 = list({num for num, count in pattern_info if count >= 2})
            if pattern_1:
                unique_identifiers = set()
                parts = identification.split('/#')
                if len(parts)==2:
                    identifier = parts[0]
                    unique_identifiers.add(identifier.strip())

            else:
                pattern_id_col = 'hid'  # Column where identifiers exist

                # Extract unique identifiers from PatternIdentification+
                unique_identifiers = set()
                parts = identification.split('/#')
                if '&' in parts[0]:
                    components = parts[0].split('&')  # Store split values in a variable
                    component_suffixes_1 = get_component_suffixes(components[0].strip(), config)
                    component_suffixes_2 = get_component_suffixes(components[1].strip(), config)

                    for identifier in parts[1].split(','):
                        identifier = identifier.strip()  # Ensure no extra spaces
                        for suffix in component_suffixes_1 + component_suffixes_2:  # Combine both suffix lists
                            unique_identifiers.add(identifier + suffix)
                else:
                    for identifier in parts[1].split(','):
                        unique_identifiers.add(identifier.strip())

                    # updated_identifiers = set()
                    # for identifier in unique_identifiers:
                    #     # Replace "BRAD" with "RBAD"
                    #     new_id = identifier.replace("BRAD", "RBAD")
                        
                    #     # Ensure the identifier follows the expected format
                    #     if not re.search(r'\D-\d+', new_id):  # Check if it already contains the correct format
                    #         new_id = re.sub(r'(\D+)(\d+)', r'\1-\2', new_id)

                    #     updated_identifiers.add(new_id)
                    # unique_identifiers = updated_identifiers

                for pattern in pattern_2:
                    matching_rows = df_hid[df_hid['pattern_no'] == pattern]
                    matched_ids = {extract_base_identifier(mid) for mid in matching_rows[pattern_id_col].tolist()}

                    # Normalize unique_identifiers
                    normalized_identifiers = {extract_base_identifier(uid) for uid in unique_identifiers}
                    if len(normalized_identifiers) == len(matched_ids):
                        # Check if all unique identifiers exist in matched_ids
                        if all(any(identifier in mid for mid in matched_ids) for identifier in normalized_identifiers):
                            return pattern
                    else:
                        if all(any(identifier in mid for mid in matched_ids) for identifier in normalized_identifiers):
                            return pattern


        
            single_pattern = [num for num, count in pattern_info if count == 1]
            if single_pattern:
                return max(single_pattern) 
    
        print(f"Error: The pattern number is not found for identification: {identification}")
        return None
        
#  Fetch mould data
def fetch_mould_data():
    today_date = datetime.datetime.today().strftime('%Y-%m-%d')

    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_queries = file.read()


    # Replace the full timestamp condition dynamically
    dataframes = []

    connection = engine.connect()  # Open connection explicitly
    try:
        for statement in sql_queries.split(";"):
            statement = statement.strip()
            if not statement:
                continue
            try:
                result = connection.execute(text(statement))
                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())
                    print(df)
                    if not df.empty:
                        print(f" Query Successful: {len(df)} rows fetched")
                        dataframes.append(df)
            except Exception as e:
                print(f" SQL Execution Error: {e}")
    finally:
        connection.close()  # ✅ Explicitly close the connection

    return pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

def clean_json_data(df):
    df = df.copy()
    print(df.columns)
    # ✅ Ensure `date` is a numeric timestamp before conversion
    if "date" in df.columns:
        df["date"] = df["date"].apply(lambda x: datetime.datetime.fromtimestamp(x / (1000 if x > 1e10 else 1))
                              .strftime('%Y-%m-%d') if isinstance(x, (int, float)) else x)
    

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
            print(" Login successful!")

            #  Extract cookies
            cookies = session.cookies.get_dict()
            print(" Cookies received:", cookies)

            #  Send POST request with correctly formatted JSON
            headers = {
                "Content-Type": "application/json",
                "Cookie": f"JSESSIONID={cookies.get('JSESSIONID', '')}"
            }

            # Send JSON properly
            response = session.post(API_URL, json=json_data, headers=headers)  #  FIXED

            #  Print API response
            print(" Response Status Code:", response.status_code)
            print(" Response Body:", response.text)
        else:
            print(f" Login failed! Status Code: {login_response.status_code}, Response: {login_response.text}")
            return
    except Exception as e:
        print(f" Unexpected error: {e}")  #  Print error details

def handle_api_update(df_id, latest_record, LAST_SENT_START_TIME):
    latest_start_time = latest_record['startTime']
    latest_end_time = latest_record['endTime']
    prev_record = df_id.iloc[-2] if len(df_id) > 1 else None

    # Case 1: First run
    if LAST_SENT_START_TIME is None:
        print("🔄 First run: Sending initial data to API.")
        json_data = clean_json_data(df_id)
        send_data_to_api(json_data)
        return latest_start_time

    # Case 2: StartTime is same
    elif LAST_SENT_START_TIME == latest_start_time:
        if prev_record is not None and latest_end_time != prev_record['endTime']:
            print(f"🆕 EndTime changed for same StartTime = {latest_start_time}. Overwriting latest record.")
            json_data = clean_json_data(latest_record.to_frame().T)
            send_data_to_api(json_data)
        else:
            print("✅ No change in StartTime or EndTime. Skipping API update.")
        return LAST_SENT_START_TIME

    # Case 3: StartTime changed
    else:
        print(f"🆕 StartTime changed from {LAST_SENT_START_TIME} ➝ {latest_start_time}")

        if prev_record is not None:
            json_prev = clean_json_data(prev_record.to_frame().T)
            print(f"✏️ Overwriting previous record: StartTime = {prev_record['startTime']}")
            send_data_to_api(json_prev)

        json_latest = clean_json_data(latest_record.to_frame().T)
        print(f"➕ Inserting new record: StartTime = {latest_start_time}")
        send_data_to_api(json_latest)

        return latest_start_time


#  Process data
def process_data():
    global LAST_SENT_START_TIME      
    df_id = fetch_mould_data()
    df_hid = fetch_pattern_components()

    if df_id.empty:
        print(" No new data available. Skipping JSON update.")
        return
        
    missing_patterns=set()
    df_id = df_id[~df_id['PatternIdentification'].isna()]

    df_id['componentId'] = df_id['PatternIdentification'].apply(
        lambda x: get_best_pattern(x, df_hid['hid'].dropna().astype(str).tolist(), df_hid, config)
    )
    print(df_id)

    #  Drop NaN before conversion
    df_id.dropna(subset=["componentId"], inplace=True)
    df_id["componentId"] = df_id["componentId"].astype(int)

    for index, row in df_id.iterrows():
        if pd.isna(row["componentId"]):
            missing_patterns.add(f"Error: The pattern number is not found for identification: {row['PatternIdentification']}")

    #  Log missing pattern numbers
    for error in sorted(missing_patterns):
        print(error)

    print(df_id)  #  Debugging step

    df_id["TotalPourStatus"] = df_id["TotalPourStatus"].astype(int)

    #  Ensure `ProductionDate` exists before conversion
    if "ProductionDate" in df_id.columns:
        df_id["date"] = pd.to_datetime(df_id["ProductionDate"]).dt.strftime('%Y-%m-%d')
    else:
        print(" ERROR: 'ProductionDate' column is missing from DataFrame!")
        return  # Stop execution if missing
    
    #  Convert `StartTime` and `EndTime` to Timedelta before extracting time
    df_id["StartTime"] = pd.to_timedelta(df_id["StartTime"], errors='coerce')
    df_id["EndTime"] = pd.to_timedelta(df_id["EndTime"], errors='coerce')

    #  Extract only the `HH:MM:SS` part, removing "0 days"
    df_id["startTime"] = df_id["StartTime"].apply(lambda x: str(x).split()[-1] if pd.notna(x) else None)
    df_id["endTime"] = df_id["EndTime"].apply(lambda x: str(x).split()[-1] if pd.notna(x) else None)

    #  Map values
    df_id["noOfBoxesPoured"] = df_id["TotalPourStatus"].where(df_id["TotalPourStatus"].notna(), None)
    df_id["totalMould"] = df_id["TotalMouldMade"].where(df_id["TotalMouldMade"].notna(), None)
    df_id["unpouredMould"] = df_id["UnpouredMould"].where(df_id["UnpouredMould"].notna(), None)  
    df_id["foundryLine"] = df_id.apply(lambda x: {"pkey": 1}, axis=1)
    df_id["badBatches"] = None
    df_id["noOfBatches"] = None
    df_id.rename(columns={"Shift": "shift"}, inplace=True)

   
    #  Ensure only required columns are kept
    df_id = df_id[config["columns_required"]]
    print(df_id.dtypes)
    print(df_id)

    df_id['unpouredMould']=df_id['unpouredMould'].astype(float)

    # ✅ Process only the latest record
    latest_record = df_id.iloc[-1]

    # ✅ Call the new handler here
    LAST_SENT_START_TIME = handle_api_update(df_id, latest_record, LAST_SENT_START_TIME)

    print(f"📌 Updated Reference: Production Date = {latest_record['date']}, Start Time = {latest_record['startTime']}")


#  Run the process every `data_frequency` minutes
while True:
    try:
        process_data()
    except Exception as e:
        print(f" Unexpected error: {e}")
    time.sleep(data_frequency)
