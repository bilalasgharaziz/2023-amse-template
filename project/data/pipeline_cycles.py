import pandas as pd
import numpy as np
from sqlalchemy import create_engine


dt1_klon = r"C:\Users\BilalAsgharAziz\OneDrive - Powercloud GmbH\Documents\Data Engineering\2023-amse-template\project\datasets\Fahrrad_Zaehlstellen_Koeln_2016.csv"
dt2_rad15 = r"C:\Users\BilalAsgharAziz\OneDrive - Powercloud GmbH\Documents\Data Engineering\2023-amse-template\project\datasets\Rad_15min.csv"

df_dt1 = pd.read_csv(dt1_klon)
df_dt2 = pd.read_csv(dt2_rad15)

print(df_dt1.head(5))
print(df_dt2.head(5))

# print("Data Transformation in progress...")
#
print("Renaming  columns to english names...")
# Renaming the columns to english titles
df_dt1.rename(
    columns={
        "Jahr 2016": "Year 2016",
        "Deutzer BrÃ¼cke": "Deutzer Bridge",
        "HohenzollernbrÃ¼cke": "Hohenzollern Bridge",
        "Neumarkt":"New Market",
        "ZÃ¼lpicher StraÃŸe":"Zulpicher Strasse",
        "Bonner StraÃŸe": "Bonner Strasse",
        "Venloer StraÃŸe":"Venloer Strasse",
        "A.-SchÃ¼tte-Allee":"A.-Schuette-Allee",
        "Vorgebirgspark":"foothill park",
        "A.-Silbermann-Weg":"A.-Silbermann-Weg",
        "Stadtwald":"city forest",
        "NiederlÃ¤nder Ufer":"Dutch shore",
    },
    inplace=True,
)


df_dt2.rename(
    columns={
        "datum": "date",
        "uhrzeit_start":"time_start",
        "uhrzeit_ende":"time_end",
        "zaehlstelle":"counting station",
        "richtung_1":"direction_1",
        "richtung_2":"direction_2",
        "gesamt":"in total"
    },
    inplace=True,
)

df_dt1.replace(np.nan, 0)
df_dt2.replace(np.nan, 0)


engine = create_engine("sqlite:///cycles.db")
df_dt1.to_sql("klon", engine, if_exists="replace")
df_dt2.to_sql("rad15", engine, if_exists="replace")
