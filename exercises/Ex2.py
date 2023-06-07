import pandas as pd
from sqlalchemy import create_engine


# ----- Data Extraction-----
def extract_data_csv(data_path):
    print("Extracting data from CSV...")
    try:
        df = pd.read_csv(data_path, sep=';', low_memory=False)
    except Exception as e:
        print("Error occurred during file reading:", str(e))
        return None
    print("Data extraction complete.")
    return df


# -----Data Transformation------
def transform_data(trains_data):
    print("Performing data transformation...")
    try:
        # 1.Drop the "Status" column
        trains_data = trains_data.drop('Status', axis=1)

        # Step 2: Drop rows with invalid values
        trains_data['Laenge'] = trains_data['Laenge'].str.replace(',', '.').astype(float)
        trains_data['Breite'] = trains_data['Breite'].str.replace(',', '.').astype(float)

        # 2.Data Validation
        # Valid "Verkehr" values
        # Valid "Laenge" values
        # Valid "Breite" values
        # Valid "IFOPT" values
        # Drop rows with empty cells

        trains_data = trains_data[
            (trains_data["Verkehr"].isin(["FV", "RV", "nur DPN"])) &
            (trains_data["Laenge"].between(-90, 90)) &
            (trains_data["Breite"].between(-90, 90)) &
            (trains_data["IFOPT"].str.match(r"^[A-Za-z]{2}:\d+:\d+(?::\d+)?$"))
            ].dropna()

        # 3.Change Data Types
        data_type = {
            "EVA_NR": int,
            "DS100": str,
            "IFOPT": str,
            "NAME": str,
            "Verkehr": str,
            "Laenge": float,
            "Breite": float,
            "Betreiber_Name": str,
            "Betreiber_Nr": int
        }
        trains_data = trains_data.astype(data_type)
    except Exception as e:
        print("Error occurred during data transformation:", str(e))
        return None

    print("Data transformation complete.")
    return trains_data


# -----Load Data -------------

def load_data(trains_data, table_name):
    print("Loading data into SQLite database...")
    try:
        engine = create_engine("sqlite:///trainstops.sqlite")
        trains_data.to_sql(table_name, engine, if_exists="replace", index=False)
        print("Data loading complete.")
    except Exception as e:
        print("Error occurred during data loading:", str(e))


def driver():
    data_path = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    trains_data = extract_data_csv(data_path)
    if trains_data is not None:
        trains_data = transform_data(trains_data)
        if trains_data is not None:
            load_data(trains_data, "trainstops")
        else:
            print("Data transformation failed. Exiting...")
    else:
        print("Data extraction failed. Exiting...")


if __name__ == "__main__":
    driver()
