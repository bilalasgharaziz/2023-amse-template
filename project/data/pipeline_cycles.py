import pandas as pd
from sqlalchemy import create_engine
import numpy as mplib
import openpyxl

def extract_data_csv(file_path):
    print("Performing data extraction...")
    data_frame = pd.read_csv(file_path, sep=';')
    return data_frame


def extract_data_xls(file_path):
    print("Performing data extraction...")
    data_frame = pd.read_excel(file_path)
    return data_frame


def transform_data(data_frame, column_mapping):
    print("Performing data transformation..")

    if column_mapping:
        print("Applying column name mapping...")
        data_frame = data_frame.rename(columns=column_mapping)

    print("Replacing missing values...")
    data_frame = data_frame.replace(mplib.nan, 0)
    return data_frame


def load_data(data_frame, table_name):
    print("Performing database operations...")
    engine = create_engine(f"sqlite:///cycles.sqlite")
    data_frame.to_sql(table_name, engine, if_exists="replace")


def driver():
    data_file1 = "https://offenedaten-koeln.de/sites/default/files/Fahrrad_Zaehlstellen_Koeln_2016.csv"
    data_file2 = "https://docs.google.com/spreadsheets/d/1c2UFhtdrizPRbxWn7vNj9glfr1x77Yjb/export?format=xlsx"

    df1 = extract_data_csv(data_file1)
    column_mapping1 = {
        "Jahr 2016": "Year",
        "Deutzer Brücke": "Bridge1",
        "Hohenzollernbrücke": "Bridge2",
        "Neumarkt": "Market",
        "Zülpicher Straße": "Street1",
        "Bonner Straße": "Street2",
        "Venloer Straße": "Street3",
        "A.-Schütte-Allee": "Allee",
        "Vorgebirgspark": "Park",
        "A.-Silbermann-Weg": "Weg",
        "Stadtwald": "Forest",
        "Niederländer Ufer": "Shore",
    }

    df1 = transform_data(df1, column_mapping1)
    load_data(df1, "data1_table")

    df2 = extract_data_xls(data_file2)
    column_mapping2 = {
        "datum": "Date",
        "uhrzeit_start": "Start Time",
        "uhrzeit_ende": "End Time",
        "zaehlstelle": "Station",
        "richtung_1": "Direction1",
        "richtung_2": "Direction2",
        "gesamt": "Total",
    }

    df2 = transform_data(df2, column_mapping2)
    load_data(df2, "data2_table")

if __name__ == "__main__":
    driver()
