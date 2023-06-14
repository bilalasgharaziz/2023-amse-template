from sqlalchemy import create_engine, inspect
import pandas as pd

from data.pipeline_cycles import (
    extract_data_csv as ex_data_csv,
    extract_data_xls as ex_data_xls,
    transform_data as tr_data
)


def test_extraction_C(file_path):
    df = ex_data_csv(file_path)
    assert not df.empty, "CSV Extraction Failed"
    print("test_extraction: Test Passed")
    return df


def test_extraction_X(file_path):
    df = ex_data_xls(file_path)
    assert not df.empty, "CSV Extraction Failed"
    print("test_extraction: Test Passed")
    return df


def test_transformation(data, rename_columns):
    df = tr_data(data, rename_columns)
    assert df.isna().any().any() == False, "NAN Found in Data"
    print("test_transformation: Test Passed")
    return df


def test_data_loading(table_name):
    engine = create_engine("sqlite:///data/cycles.db")

    # Create an inspector object
    inspector = inspect(engine)

    # Check if a table exists in the database
    exists = inspector.has_table(table_name)
    assert exists, f"The table '{table_name}' does not exist in the database."
    print("test_data_loading: Table '" + table_name + "' exists, Test Passed")


def test_pipeline():
    file_path1 = "https://offenedaten-koeln.de/sites/default/files/Fahrrad_Zaehlstellen_Koeln_2016.csv"
    df1 = test_extraction_C(file_path1)
    df1 = test_transformation(df1, {"Jahr 2016": "Year",
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
                                    "Niederländer Ufer": "Shore", })

    test_data_loading("data1_table")

    file_path2 = "https://docs.google.com/spreadsheets/d/1c2UFhtdrizPRbxWn7vNj9glfr1x77Yjb/export?format=xlsx"
    df2 = test_extraction_X(file_path2)
    df2 = test_transformation(df2, {
        "datum": "Date",
        "uhrzeit_start": "Start Time",
        "uhrzeit_ende": "End Time",
        "zaehlstelle": "Station",
        "richtung_1": "Direction1",
        "richtung_2": "Direction2",
        "gesamt": "Total"})

    test_data_loading("data2_table")


if __name__ == "__main__":
    test_pipeline()
