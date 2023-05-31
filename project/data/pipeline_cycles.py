import pandas as pd
from sqlalchemy import create_engine
import numpy as mplib

def extract_data(file_path):
    print("Performing data extraction...")
    data_frame = pd.read_csv(file_path)
    return data_frame


def transform_data(data_frame, column_mapping):
    print("Performing data transformation...")

    if column_mapping:
        print("Applying column name mapping...")
        data_frame = data_frame.rename(columns=column_mapping)

    print("Replacing missing values...")
    data_frame = data_frame.replace(mplib.nan, 0)
    return data_frame


def load_data(data_frame, table_name):
    print("Performing database operations...")
    engine = create_engine("sqlite:///cycles.db")
    data_frame.to_sql(table_name, engine, if_exists="replace")


def driver():
    data_file1 = r"C:\Users\BilalAsgharAziz\OneDrive - Powercloud GmbH\Documents\Data Engineering\2023-amse-template\project\datasets\Fahrrad_Zaehlstellen_Koeln_2016.csv"
    data_file2 = r"C:\Users\BilalAsgharAziz\OneDrive - Powercloud GmbH\Documents\Data Engineering\2023-amse-template\project\datasets\Rad_15min.csv"

    df1 = extract_data(data_file1)
    column_mapping1 = {
        "Year 2016": "Year",
        "Deutzer Bridge": "Bridge1",
        "Hohenzollern Bridge": "Bridge2",
        "New Market": "Market",
        "Zulpicher Strasse": "Street1",
        "Bonner Strasse": "Street2",
        "Venloer Strasse": "Street3",
        "A.-Schuette-Allee": "Allee",
        "Foothill Park": "Park",
        "A.-Silbermann-Weg": "Weg",
        "City Forest": "Forest",
        "Dutch Shore": "Shore",
    }


    df1 = transform_data(df1, column_mapping1)
    load_data(df1, "data1_table")

    df2 = extract_data(data_file2)
    column_mapping2 = {
        "date": "Date",
        "time_start": "Start Time",
        "time_end": "End Time",
        "counting_station": "Station",
        "direction_1": "Direction1",
        "direction_2": "Direction2",
        "in_total": "Total",
    }

    df2 = transform_data(df2, column_mapping2)
    load_data(df2, "data2_table")


if __name__ == "__main__":
    driver()