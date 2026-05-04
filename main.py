import pandas as pd
# import yaml
from sqlalchemy import create_engine
from sqlalchemy.types import Date, Time, Float, String


# # Our password
# with open("secrets.yml", "r") as file:
#     secrets = yaml.safe_load(file)
# PW = secrets.get("PW")

# dataset url
url = "https://raw.githubusercontent.com/fivethirtyeight/uber-tlc-foil-response/master/uber-trip-data/uber-raw-data-apr14.csv"

# load csv
df = pd.read_csv(url)

# rename columns to match mysql table
df = df.rename(columns={
    "Date/Time": "pickup_datetime",
    "Lat": "latitude",
    "Lon": "longitude",
    "Base": "base_code"
})

# Uber dataset: M/D/YYYY H:MM:SS --> MySQL format: YYYY-MM-DD HH:MM:SS
df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
df["pickup_date"] = df["pickup_datetime"].dt.strftime("%Y-%m-%d")
df["pickup_time"] = df["pickup_datetime"].dt.strftime("%H:%M:%S")

df = df.drop(columns=["pickup_datetime"])   

# connection
engine = create_engine(
    f"mysql+pymysql://lipesszy:VX9LUePzs5eEGTUh@mysql.agh.edu.pl/lipesszy"
)

# insert data
df.to_sql(
    "uber_pickups",
    con=engine,
    if_exists="replace",
    index=False,
    chunksize=5000,
    dtype={
        "pickup_date": Date(),
        "pickup_time": Time(),
        "latitude": Float(),
        "longitude": Float(),
        "base_code": String(10)
    }
)

print("Data inserted successfully.") 