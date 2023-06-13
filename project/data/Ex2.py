import pandas as pd
from sqlalchemy import create_engine


#----- Data Extraction-----

def extract_data_csv(path):
    print("Extracting data from CSV...")
    try:
        df = pd.read_csv(path, sep=';', low_memory=False)
    except Exception as e:
        print("Error occurred during file reading:", str(e))
        return None
    print("Data extraction complete.")
    return df


#-----Data Transformation------
def transform_data(data_frame):
    print("Performing data transformation...")
    try:
        #1.Drop the "Status" column
        data_frame = data_frame.drop('Status', axis=1)

        #2.Drop rows with invalid values
        data_frame['Laenge'] = data_frame['Laenge'].str.replace(',', '.').astype(float)
        data_frame['Breite'] = data_frame['Breite'].str.replace(',', '.').astype(float)

        #3.Data Validation
        data_frame = data_frame[
            (data_frame["Verkehr"].isin(["FV", "RV", "nur DPN"])) &  # Valid "Verkehr" values
            (data_frame["Laenge"].between(-90, 90)) &  # Valid "Laenge" values
            (data_frame["Breite"].between(-90, 90)) &  # Valid "Breite" values
            (data_frame["IFOPT"].str.match(r"^[A-Za-z]{2}:\d+:\d+(?::\d+)?$"))  # Valid "IFOPT" values
        ].dropna()  # Drop rows with empty cells

        #4.Change Data Types
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
        data_frame = data_frame.astype(data_type)
    except Exception as e:
        print("Error occurred during data transformation:", str(e))
        return None

    print("Data transformation complete.")
    return data_frame



#-----Load Data -------------

def load_data(data_frame, table_name):
    print("Loading data into SQLite database...")
    try:
        engine = create_engine("sqlite:///trainstops.sqlite")
        data_frame.to_sql(table_name, engine, if_exists="replace", index=False)
        print("Data loading complete.")
    except Exception as e:
        print("Error occurred during data loading:", str(e))


def main():
    path = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
    data_frame = extract_data_csv(path)
    if data_frame is not None:
        data_frame = transform_data(data_frame)
        if data_frame is not None:
            load_data(data_frame, "trainstops")
        else:
            print("Data transformation failed. Exiting...")
    else:
        print("Data extraction failed. Exiting...")


if __name__ == "__main__":
    main()
