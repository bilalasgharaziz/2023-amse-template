from sqlalchemy import create_engine, inspect
import pandas as pd

from data.pipeline import (
    extract_data_xls as ex_data_xls,
    extract_data_xls as ex_data_xls,
    transform_data as tr_data
)


def test_extraction_C(file_path):
    df = ex_data_xls(file_path)
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
    engine = create_engine(f"sqlite:///cycles.sqlite")

    # Create an inspector object
    inspector = inspect(engine)

    # Check if a table exists in the database
    exists = inspector.has_table(table_name)
    assert exists, f"The table '{table_name}' does not exist in the database."
    print("test_data_loading: Table '" + table_name + "' exists, Test Passed")


def test_pipeline():
    file_path1 = "https://drive.google.com/uc?export=download&id=10-H4W8QHKhlsiFwI29-pUzdtH8ZZqidU"
    df1 = test_extraction_C(file_path1)
    df1 = test_transformation(df1, {
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
        "Niederländer Ufer": "Niederländer Ufer", })

    test_data_loading("data1_table")

    file_path2 = "https://drive.google.com/uc?export=download&id=1NrYUAKJYw2EYqLxDA4IiXNJM8axdPS1p"
    df2 = test_extraction_X(file_path2)
    df2 = test_transformation(df2, {
        "Year 2017": "Year 2017",
        "Arnulf": "Arnulf",
        "Erhardt": "Erhardt",
        "Hirsch": "Hirsch",
        "Kreuther": "Kreuther",
        "Margareten": "Margareten",
        "Olympia": "Olympia",
        "Grand Total": "Grand Total"
    })

    test_data_loading("data2_table")



if __name__ == "__main__":
    test_pipeline()
