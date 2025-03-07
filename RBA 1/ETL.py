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
def get_max_pattern(identification):
    id_part = identification.split('#')[-1] if '#' in identification else identification
    id_list = id_part.split(',')

    pattern_numbers = []
    for hid in df_hid['hid']:
        if any(id_val.strip() in hid for id_val in id_list):
            try:
                pattern_number = int(hid.split('-')[-1])
                pattern_numbers.append(pattern_number)
            except ValueError:
                pass
 
    if '#' in identification:
        id_part = identification.split('#')[-1]
        parts = id_part.split()

        combined_terms = []

      
        for i, part in enumerate(parts):
            if re.match(r'^[A-Za-z]+-\d{4}$', part):
                alphanumeric_part, numeric_part = part.split('-')
                base_term = f'{alphanumeric_part}-{numeric_part}'
                combined_terms.extend([numeric_part, base_term]) 


                if i + 1 < len(parts):
                    next_part = parts[i + 1]
                    normalized_next_part = re.sub(r'[^A-Za-z]', '', next_part)
                    combined_with_suffix = f'{base_term}{normalized_next_part}' 
                    combined_terms.append(combined_with_suffix)

                break

        combined_terms = sorted(combined_terms, key=len, reverse=True)
        found = False  
        for term in combined_terms:
            for hid in df_hid['hid']:
                if term in hid:
                    try:
                        pattern_number = int(hid.split('-')[-1])
                        pattern_numbers.append(pattern_number)
                        found = True 
                        break
                    except ValueError:
                        pass
            if found:  
                break


    if not pattern_numbers:
        id_part = identification.split('#')[-1] if '#' in identification else identification
        id_part = re.sub(r'\s*\(.*?\)', '', id_part).strip()
        id = id_part.split()[-1]
        id_list = ''.join(id.split())
        numeric_part = ''.join([char for char in id_list if char.isdigit()])
        for hid in df_hid['hid']:
            if numeric_part in hid:
                try:
                    pattern_number = int(hid.split('-')[-1])
                    pattern_numbers.append(pattern_number)
                except ValueError:
                    pass



    if pattern_numbers:
        return max(pattern_numbers)
    else:
        print(f"Error: The pattern number is not found for identification: {identification}")
        return None

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