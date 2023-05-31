from sqlalchemy import create_engine, inspect
import pandas as pd

from data.pipeline_cycles import (
    extract_data as ex_data,
    transform_data as tr_data
)


def test_extraction(file_path):
    df = ex_data(file_path)
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
    file_path1 = r"C:\Users\BilalAsgharAziz\OneDrive - Powercloud GmbH\Documents\Data Engineering\2023-amse-template\project\datasets\Fahrrad_Zaehlstellen_Koeln_2016.csv"
    df1 = test_extraction(file_path1)
    df1 = test_transformation(df1, {"Year 2016": "Year",
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
                                    "Dutch Shore": "Shore"})
    test_data_loading("data1_table")

    file_path2 = r"C:\Users\BilalAsgharAziz\OneDrive - Powercloud GmbH\Documents\Data Engineering\2023-amse-template\project\datasets\Rad_15min.csv"
    df2 = test_extraction(file_path2)
    df2 = test_transformation(df2, {
        "date": "Date",
        "time_start": "Start Time",
        "time_end": "End Time",
        "counting_station": "Station",
        "direction_1": "Direction1",
        "direction_2": "Direction2",
        "in_total": "Total"})

    test_data_loading("data2_table")


if __name__ == "__main__":
    test_pipeline()
