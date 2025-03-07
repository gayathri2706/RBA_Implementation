import pandas as pd
import numpy as np
import json
import os
import time
import warnings
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

# Extract database credentials
db_config = config["database"]
sql_file_path = config["sql_file_path"]
data_frequency = config["data_frequency"]

DB_USER = db_config["user"]
DB_PASSWORD = db_config["password"]
DB_HOST = db_config["host"]
DB_PORT = db_config["port"]
DB_NAME = db_config["database_name"]

# Establish database connection using SQLAlchemy
engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


# # Initialize DataFrame for storing last reference ID in memory
# last_ref_id_df = pd.DataFrame(columns=["Production Date", "Start Time"])

# def get_last_ref_id():
#     """Fetch the last reference Production Date and Start Time from memory."""
#     if not last_ref_id_df.empty:
#         return last_ref_id_df.iloc[-1]["Production Date"], last_ref_id_df.iloc[-1]["Start Time"]
#     return None, None  # Return None if DataFrame is empty

# def update_ref_id(production_date, start_time):
#     """Update the last reference Production Date and Start Time in memory."""
#     global last_ref_id_df
#     new_entry = pd.DataFrame({"Production Date": [production_date], "Start Time": [start_time]})
#     last_ref_id_df = pd.concat([last_ref_id_df, new_entry], ignore_index=True)
#     print(f" Updated Reference: Production Date = {production_date}, Start Time = {start_time}")

def fetch_mould_data():
    """Fetch mould data from SQL file and return as DataFrame."""
    
    # last_production_date, last_start_time = get_last_ref_id()  # Fetch last reference ID

    df_id = pd.DataFrame()  # Initialize empty DataFrame
    dataframes = []  # List to store results

    # Read SQL queries from file
    with open(sql_file_path, "r", encoding="utf-8") as file:
        sql_queries = file.read()

    with engine.connect() as connection:
        for statement in sql_queries.split(";"):  # Split multiple queries
            statement = statement.strip()
            if not statement:
                continue  # Skip empty queries

            try:
                result = connection.execute(text(statement))  # Execute SQL query

                # Convert to DataFrame if data is returned
                if result.returns_rows:
                    df = pd.DataFrame(result.fetchall(), columns=result.keys())

                    if not df.empty:
                        print(f"✅ Query Successful: {len(df)} rows fetched")
                        print(df.head())  # Display first few rows for debugging

                        # # Filter new data based on last reference
                        # new_data = df[
                        #     (df["Production Date"] > last_production_date) |
                        #     ((df["Production Date"] == last_production_date) & (df["Start Time"] > last_start_time))
                        # ]

                        # if not new_data.empty:
                        #     dataframes.append(new_data)  # Store new data
                        #     print(f"🔹 New Data Found: {len(new_data)} rows")
                        dataframes.append(df)  # Store new data
            except Exception as e:
                print(f"❌ SQL Execution Error: {e}")

    # Combine all data into a single DataFrame
    df_id = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

    # # Update last reference ID if new data is available
    # if not df_id.empty:
    #     update_ref_id(df_id["Production Date"].max(), df_id["Start Time"].max())

    return df_id

def get_max_pattern(identification):
    id_part = identification.split('#')[-1] if '#' in identification else identification
    id_list = id_part.split(',')

    pattern_numbers = []
    for hid in df_hid['Hid']:
        if any(id_val.strip() in hid for id_val in id_list):
            try:
                pattern_number = int(hid.split('-')[-1])
                pattern_numbers.append(pattern_number)
            except ValueError:
                pass

    if not pattern_numbers:
        id_part = identification.split('#')[-1] if '#' in identification else identification
        id_list=id_part.split()[0]
        numeric_part = ''.join([char for char in id_list if char.isdigit()])
        for hid in df_hid['Hid']:
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

def process_data():
    """Process fetched mould data and save to JSON."""
    df_id = fetch_mould_data()
    print(df_id)

    if df_id.empty:
        print("No new data available. Skipping JSON update.")
        return  # Don't write placeholder null values

    df_id["Pattern number"] = df_id["PatternIdentification"].apply(get_max_pattern)
    df_id = df_id[config["columns_required"]]
    
    # Save as JSON
    df_id.to_json("pattern_data.json", orient="records", indent=4)
    print(" Data saved successfully in JSON format: pattern_data.json")

# Run the loop every 10 minutes
while True:
    try:
        process_data()
    except Exception as e:
        print(f"Unexpected error: {e}")

    time.sleep(data_frequency)
