import pandas as pd
import yaml
from sqlalchemy import create_engine

# Our password
with open("secrets.yml", "r") as file:
    secrets = yaml.safe_load(file)
PW = secrets.get("PW")

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

# connection
engine = create_engine(
    f"mysql+pymysql://lipesszy:{PW}@mysql.agh.edu.pl/lipesszy"
)

# insert data
df.to_sql(
    "uber_pickups",
    con=engine,
    if_exists="append",
    index=False,
    chunksize=5000  #lots of data
)

print("Data inserted successfully.") 