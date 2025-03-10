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

# ✅ Function to find pattern number
def get_pattern_number(identification, hid_list):
    pattern_numbers = []
    
    id_list = identification.split('/#')[-1].split(',')
    id_list = [item.strip().replace(" ", "") for item in id_list]
    
    for id_entry in id_list:
        matching_patterns = [int(hid.split('-')[-1]) for hid in hid_list if id_entry in hid]
        if matching_patterns:
            pattern_numbers.append(max(matching_patterns))

    return max(pattern_numbers) if pattern_numbers else None

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

    # ✅ Apply pattern search
    missing_patterns = set()
    df_id["Pattern number"] = df_id["PatternIdentification"].apply(lambda x: get_pattern_number(x, df_hid['hid'].tolist()))

    for index, row in df_id.iterrows():
        if pd.isna(row["Pattern number"]):
            missing_patterns.add(f"Error: The pattern number is not found for identification: {row['PatternIdentification']}")

    # ✅ Log missing pattern numbers
    for error in sorted(missing_patterns):
        print(error)

    # ✅ Save to JSON
    df_id = df_id[config["columns_required"]]
    df_id.to_json("df_pattern.json", orient="records", indent=4)
    print("Data saved successfully in JSON format: df_pattern.json")

    # ✅ Print reference update
    last_row = df_id.iloc[-1] if not df_id.empty else None
    if last_row is not None:
        print(f"🔄 Updated Reference: Production Date = {last_row['ProductionDate']}, Start Time = {last_row['StartTime']}")

# ✅ Run the process every `data_frequency` minutes
while True:
    try:
        process_data()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
    time.sleep(data_frequency)
