import pandas as pd
from sqlalchemy import create_engine
import numpy as mplib
import openpyxl

def extract_data_xls(file_path):
    print("Performing data extraction...")
    data_frame = pd.read_excel(file_path, sep=';')
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
    data_file1 = "https://drive.google.com/uc?export=download&id=10-H4W8QHKhlsiFwI29-pUzdtH8ZZqidU"
    data_file2 = "https://drive.google.com/uc?export=download&id=1NrYUAKJYw2EYqLxDA4IiXNJM8axdPS1p"
    data_file3 = "https://drive.google.com/uc?export=download&id=1WWyeZ8X0jX6PpVsAXQEzg81HbT61K3ks"
    data_file4 = "https://drive.google.com/uc?export=download&id=13-xC9oNVYNiWOcuGP2KjGC4mWoO_1T6X"
    data_file5 = "https://drive.google.com/uc?export=download&id=11Cn2cdKyXhz9RHAnRhTKFok_fkWvNdsl"
    data_file6 = "https://drive.google.com/uc?export=download&id=1bLo2hC2TXPdrK5oyfsGECTgouUJC1NRw"

    df1 = extract_data_xls(data_file1)
    column_mapping1 = {
        "Year 2017": "Year 2017",
        "Deutzer Brücke": "Deutzer Brücke",
        "Hohenzollernbrücke": "Hohenzollernbrücke",
        "Neumarkt": "Neumarkt",
        "Zülpicher Straße": "Zülpicher Straße",
        "Bonner Straße": "Bonner Straße",
        "Venloer Straße": "Venloer Straße",
        "A.-Schütte-Allee": "A.-Schütte-Allee",
        "Vorgebirgspark": "Vorgebirgspark",
        "A.-Silbermann-Weg": "A.-Silbermann-Weg",
        "Stadtwald": "Stadtwald",
        "Niederländer Ufer": "Niederländer Ufer",
    }

    df1 = transform_data(df1, column_mapping1)
    load_data(df1, "data1_table")

    df2 = extract_data_xls(data_file2)
    column_mapping2 = {
        "Year 2017": "Year 2017",
        "Arnulf": "Arnulf",
        "Erhardt": "Erhardt",
        "Hirsch": "Hirsch",
        "Kreuther": "Kreuther",
        "Margareten": "Margareten",
        "Olympia": "Olympia",
        "Grand Total": "Grand Total",
    }

    df2 = transform_data(df2, column_mapping2)
    load_data(df2, "data2_table")

    df3 = extract_data_xls(data_file3)
    column_mapping3 = {
        "Streets": "Streets",
        "latitude": "latitude",
        "longitude": "longitude",
        "Grand Total": "Grand Total",
    }

    df3 = transform_data(df3, column_mapping3)
    load_data(df3, "data3_table")

    df4 = extract_data_xls(data_file4)
    column_mapping4 = {
        "Streets": "Streets",
        "latitude": "latitude",
        "longitude": "longitude",
        "Grand Total": "Grand Total",
    }

    df4 = transform_data(df4, column_mapping4)
    load_data(df4, "data4_table")

    df5 = extract_data_xls(data_file5)
    column_mapping5 = {
        "Month": "Month",
        "Arnulf": "Arnulf",
        "Erhardt": "Erhardt",
        "Hirsch": "Hirsch",
        "Kreuther": "Kreuther",
        "Margareten": "Margareten",
        "Olympia":"Olympia",
        "Grand Total":"Grand Total"
    }

    df5 = transform_data(df5, column_mapping5)
    load_data(df5, "data5_table")

    df6 = extract_data_xls(data_file6)
    column_mapping6 = {
        "Month": "Month",
        "Hohenzollernbrücke": "Hohenzollernbrücke",
        "Neumarkt": "Neumarkt",
        "Zülpicher Straße": "Zülpicher Straße",
        "Bonner Straße": "Bonner Straße",
        "Venloer Straße": "Venloer Straße",
        "A.-Schütte-Allee":"A.-Schütte-Allee",
        "Vorgebirgspark":"Vorgebirgspark",
        "A.-Silbermann-Weg": "A.-Silbermann-Weg",
        "Stadtwald":"Stadtwald","Stadtwald":"Stadtwald",
        "Niederländer Ufer":"Niederländer Ufer"
    }

    df6 = transform_data(df6, column_mapping6)
    load_data(df6, "data6_table")

if __name__ == "__main__":
    driver()
