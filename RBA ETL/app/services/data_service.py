import numpy as np
import pandas as pd
import datetime, json
import pymysql
import os
import traceback

from datetime import timedelta, datetime
from typing import List, Tuple
from functools import lru_cache

from flask import g as global_thread_local
from flask import current_app as app

from sqlalchemy import and_, func
from sqlalchemy import create_engine

from ..entities.models import PreparedSand, SMCData, Customer, ConsumptionBooking, FoundryLine
from ..errors.exceptions import SandmanError


from etl import get_last_processed_timestamp

def get_smc_data(line_id, from_date, to_date, config):
    cond = and_(SMCData.foundry_line_id == line_id, SMCData.date >= from_date,
                SMCData.date <= to_date)
    qry = global_thread_local.SESSION.query(SMCData).filter(cond)
    smc_data = qry.all()
    app.logger.info("total smc row: %d", len(smc_data))
    if smc_data is None or len(smc_data) == 0:
        raise SandmanError("SMC data not found")

    smc_df = pd.DataFrame([obj.__dict__ for obj in smc_data])
    smc_df.drop(['_sa_instance_state'], axis=1, inplace=True)

    #smc_df['date'] = smc_df['date'].astype(str)
    #smc_df['datetime'] = pd.to_datetime(smc_df['date']) + pd.to_timedelta(smc_df['time'])
    smc_df['datetime'] = smc_df.apply(
        lambda row: datetime.combine(row['date'], row['time']),
        axis=1
    )
    smc_df['batch_counter'] = smc_df.groupby(config['Batch_reset']).cumcount() + 1
    smc_df['datetime'] = pd.to_datetime(smc_df['datetime'], format='%Y-%m-%d %H:%M')
    smc_df = smc_df.sort_values('datetime')

    print(smc_df.head(5))
    return smc_df


def read_customer(customer_id):
    customer = global_thread_local.SESSION.query(Customer).get(customer_id)
    return customer


def get_foundry_line(line_id):
    qry = global_thread_local.SESSION.query(FoundryLine).filter(FoundryLine.pkey == line_id)
    foundry_line = qry.one()
    # print(dir(foundry_line))
    return foundry_line


def get_consumption_bookings(line_id, from_date, to_date):
    cond = and_(ConsumptionBooking.foundry_line_id == line_id, ConsumptionBooking.date >= from_date,
                ConsumptionBooking.date <= to_date)

    qry = global_thread_local.SESSION \
        .query(ConsumptionBooking).filter(cond)

    cons_booking_db_data = qry.all()
    app.logger.info("total consumption bookings: %d", len(cons_booking_db_data))

    cons_booking_df = pd.DataFrame([obj.__dict__ for obj in cons_booking_db_data])
    cons_booking_df.drop(['_sa_instance_state', 'version', 'created_by', 'created_on', 'pkey', 'deleted',
                  'last_updated_by', 'last_updated_on', 'temp_line', 'next_running_component', 'foundry_line_group_id',
                  'foundry_mixer_id', 'foundry_line_id'], axis=1, inplace=True)

    print(cons_booking_df.dtypes)
    print(cons_booking_df.head(5))

    column_mapping = {
        "date": "Date",
        "shift": "Shift",
        "component_id": "ComponentId",
        "start_time": "StartTime",
        "end_time": "EndTime",
        "total_mould": "Total Mould Made",
        "unpoured_mould": "Unpoured Mould",
        "newsand_sp": "New Sand Set Point",
        "bentonite_sp": "Bentonite Set Point",
        "coaldust_sp": "Coal Dust Set Point",
        "water_sp": "Water Set Point",
        "no_of_boxes_poured": "No of Boxes Poured",
        "bad_batches": "Bad Batches",
        "no_of_batches": "No of Batches"
    }

    cons_booking_df = cons_booking_df[list(column_mapping.keys())]
    cons_booking_final_df = cons_booking_df.rename(columns=column_mapping)
    print(cons_booking_final_df.dtypes)
    print(cons_booking_final_df.head(5))
    return cons_booking_df


def get_config():
    cd = os.getcwd()
    print(cd)
    config_dir = os.path.join(cd, "Config")
    config_file_path = os.path.join(config_dir, "config.json")

    # Load configuration
    with open(config_file_path, "r") as config_file:
        config = json.load(config_file)

    return config


def get_additive_raw_data(line_id, config, connection):
    df_add = pd.read_sql("SELECT * FROM additive_data_v2", connection)

    df_add = date_adjust(df_add, "datetime", True, config)

    # Define column pairs for cleaning
    column_pairs = [
        ("New_Sand_Set_kgs", "New_Sand_Act_Kgs"),
        ("Bentonite_Set_Kgs", "Bentonite_Act_Kgs"),
        ("Return_Sand_Set_Kgs", "Return_Sand_Act_Kgs"),
        ("Fine_Dust_Set_Kgs", "Fine_Dust_Act_Kgs"),
        ("Coal_Dust_Set_Kgs", "Coal_Dust_Act_Kgs"),
        ("Water_Dosing_Set_Litre", "Water_Dosing_Act_Litre"),
    ]

    df_add = clean_actual_columns(df_add, column_pairs)

    # Rename columns according to config
    column_renaming = config["columns_to_rename"]
    df_add.rename(columns=column_renaming, inplace=True)
    print("Renamed columns:", df_add.columns.tolist())
    df_add = df_add.sort_values('datetime')
    print(df_add.head(5))
    return df_add


def date_adjust(data: pd.DataFrame, col_name: str, to_foundry: bool, config):
    shift_cutoff = pd.to_datetime(config["shift_time"]["A"][0]).time()
    multiplier = 1 if to_foundry else -1

    time_mask = data[col_name].dt.time < shift_cutoff
    data["datetime"] = data["datetime"] + pd.to_timedelta(time_mask.astype(int) * -1 * multiplier, unit='D')
    return data


def clean_actual_columns(df: pd.DataFrame, column_pairs: List[Tuple[str, str]]) -> pd.DataFrame:
    for set_col, act_col in column_pairs:
        df[set_col] = pd.to_numeric(df[set_col], errors='coerce')
        df[act_col] = pd.to_numeric(df[act_col], errors='coerce')

        mask = (df[set_col] != 0) & (df[act_col] <= 0)
        df.loc[mask, act_col] = np.nan

        temp_col = df[act_col].replace(0, np.nan)
        filled = temp_col.ffill().bfill()
        df[act_col] = df[act_col].combine_first(filled)
        df.loc[df[set_col] == 0, act_col] = 0

    return df


def run_etl(line_id, from_date, to_date):
    response = dict()
    connection = None

    try:
        line = get_foundry_line(line_id)
        scada_db_props = json.loads(line.scada_db_properties_json)
        print(scada_db_props)

        # Database connection parameters
        # db_config = scada_db_props["dbName"]
        DB_USER = scada_db_props["username"]
        DB_PASSWORD = scada_db_props["password"]
        DB_HOST = scada_db_props["host"]
        DB_PORT = scada_db_props["port"]
        DB_NAME = scada_db_props["dbName"]

        # Create database engine for pandas
        # engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

        # Create a direct pymysql connection for more efficient execution and transactions
        connection = pymysql.connect(
            host=DB_HOST,
            port=int(DB_PORT),
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            cursorclass=pymysql.cursors.DictCursor
        )

        df, latest_timestamp = process_additive_etl(line_id, from_date, to_date, connection)

        column_mapping = {
            "timestamp": "date",
            "shift": "shift",
            "mixer_name": "mixerName",
            "batch_counter": "batchCounter",
            "component_id": "componentId",
            "recycle_sand_set_point": "recycleSandSetPoint",
            "recycle_sand_actual": "recycleSandActual",
            "bentonite_set_point": "bentoniteSetPoint",
            "bentonite_actual": "bentoniteActual",
            "coal_dust_set_point": "coalDustSetPoint",
            "coal_dust_actual": "coalDustActual",
            "fss_set_point": "fssSetPoint",
            "fss_actual": "fssActual",
            "water_set_point": "waterSetPoint",
            "water_actual": "waterActual",
            "inert_fines_set_point": "inertFinesSetPoint",
            "inert_fines_actual": "inertFinesActual",
            "compactability_smc_pct": "compactabilitySmcPct",
            "cosp_percentage_pct": "cospPercentagePct",
            "temperature_c": "temperatureC",
            "total_seconds": "totalSeconds",
            "total_water_ltr": "totalWaterLtr",
            "moisture_smc_pct": "moistureSmcPct",
            "wd1_ltr": "wd1Ltr",
            "co1_pct": "co1Pct"
        }
        df = df.rename(columns=column_mapping)

        response["status"] = 200
        response["message"] = "success"
        response['latest_timestamp'] = latest_timestamp
        response['additive_data'] = df.to_dict(orient='records')
    except Exception as e:
        app.logger.error("ETL failed:", e)
        response["status"] = 500
        response["message"] = "ETL failed: " + e
        traceback.print_exc()
    finally:
        if connection is not None:
            connection.close()
            app.logger.info("Database connection closed.")

    return response



def process_additive_etl(line_id, from_date, to_date, connection):
    config = get_config()

    df_add = get_additive_raw_data(line_id, config, connection)
    smc_df = get_smc_data(line_id, from_date, to_date, config)
    prod_data = get_consumption_bookings(line_id, from_date, to_date)

    last_timestamp = get_last_processed_timestamp(connection)
    if last_timestamp:
        app.logger.info(f"Last processed timestamp: {last_timestamp}")
    else:
        app.logger.info("First run or no timestamp found in logger.")

    # Merge the datasets using datetime as the key
    matched_df = pd.merge_asof(
        smc_df,
        df_add,
        on='datetime',
        direction='nearest',
    )

    # Extract temporary date and time parts for processing
    matched_df['date'] = matched_df['datetime'].dt.date
    matched_df['time'] = matched_df['datetime'].dt.time
    matched_df = matched_df.sort_values(['date', 'time'])

    # Process product data for component lookup
    print("Processing component data...")
    #prod_data['StartTime'] = pd.to_datetime(prod_data['date'] + pd.to_timedelta(prod_data['start_time']))
    prod_data['StartTime'] = prod_data.apply(lambda row: datetime.combine(row['date'], row['start_time']), axis=1)
    prod_data['EndTime'] = prod_data.apply(lambda row: datetime.combine(row['date'], row['end_time']), axis=1)
    #prod_data['EndTime'] = pd.to_datetime(prod_data['date'] + pd.to_timedelta(prod_data['end_time']))

    # Fix end times that cross midnight
    midnight_crossings = prod_data['EndTime'] < prod_data['StartTime']
    prod_data.loc[midnight_crossings, 'EndTime'] += pd.Timedelta(days=1)

    component_ranges = []
    for _, row in prod_data.iterrows():
        component_ranges.append({
            'start': row['StartTime'],
            'end': row['EndTime'],
            'component_id': row['component_id'],
            'span_hours': (row['EndTime'] - row['StartTime']).total_seconds() / 3600
        })

    @lru_cache(maxsize=1000)
    def get_component_id(dt_str):
        dt = pd.Timestamp(dt_str)
        dt_next_day = dt + pd.Timedelta(days=1)

        # First try with the original datetime
        for comp_range in component_ranges:
            if comp_range['start'] <= dt <= comp_range['end']:
                return comp_range['component_id']

        # For multi-day components, check next day
        for comp_range in component_ranges:
            if comp_range['span_hours'] > 12:  # Only check for long-running components
                if comp_range['start'] <= dt_next_day <= comp_range['end']:
                    return comp_range['component_id']

        return None

    # Apply the component lookup function
    print("Matching components to time ranges...")
    matched_df['component_id'] = matched_df['datetime'].astype(str).apply(get_component_id)
    matched_df['mixer_name'] = config['Mixer Name']

    matched_df['shift'] = matched_df['datetime'].apply(lambda dt: assign_shift(dt, config))

    # Create the timestamp column
    matched_df['timestamp'] = matched_df.apply(compute_timestamp, axis=1)

    # List of columns to select (excluding date and time, including timestamp)
    columns_to_select = [col for col in config["columns_to_select"] if col not in ['date', 'time']]
    columns_to_select.insert(0, 'timestamp')  # Add timestamp as the first column

    # Check if all required columns exist
    missing_columns = [col for col in columns_to_select if col not in matched_df.columns]
    if missing_columns:
        print(f"Warning: Missing columns in dataframe: {missing_columns}")
        print("Available columns:", matched_df.columns.tolist())

        # Fill missing columns with NaN
        for col in missing_columns:
            matched_df[col] = np.nan

    # Select only the needed columns
    df = matched_df[columns_to_select]

    # Sort by timestamp
    df = df.sort_values(by=['timestamp'])

    # Rename columns to match the target database schema
    output_columns = config["output_columns"].copy()

    # Ensure timestamp is included in output columns mapping
    if 'timestamp' not in output_columns:
        output_columns['timestamp'] = 'timestamp'

    # Apply column renaming
    df.rename(columns=output_columns, inplace=True)

    # Filter out records that already exist in the database
    if last_timestamp is not None:
        new_records = df[df['timestamp'] > last_timestamp]
        old_records = df[df['timestamp'] <= last_timestamp]
        print(f"Total records: {len(df)}")
        print(f"Records already in database: {len(old_records)}")
        print(f"New records to insert: {len(new_records)}")
        df = new_records

    # If no new records, return empty dataframe with the latest timestamp
    if len(df) == 0:
        print("No new records to insert.")
        return df, None

    # Get the latest timestamp from the new records
    latest_timestamp = df['timestamp'].max()

    # Show sample data
    print("Sample of new data to be inserted:")
    print(df.head())

    # Check column names match the target schema
    print("Final columns for DB insert:", df.columns.tolist())
    return df, latest_timestamp



# Assign shift based on time
def assign_shift(row, config):
    shift_a_start = datetime.strptime(config["shift_time"]["A"][0], "%H:%M:%S").time()
    shift_b_start = datetime.strptime(config["shift_time"]["B"][0], "%H:%M:%S").time()

    if shift_a_start <= row.time() < shift_b_start:
        return 'A'
    else:
        return 'B'


# Create the timestamp column properly accounting for shift
def compute_timestamp(row):
    base_datetime = datetime.combine(row['date'], row['time'])

    # Adjust for B shift times after midnight
    if row['shift'] == 'B' and row['time'] < datetime.strptime("07:00", "%H:%M").time():
        return base_datetime + timedelta(days=1)
    else:
        return base_datetime