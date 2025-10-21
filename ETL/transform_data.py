import json
import pandas as pd

#
def transform(data): 
    try:
        # mise au format dataframe pandas
        df = pd.json_normalize(data)

        # correction de l'heure sous le bon format
        df['datetime'] = pd.to_datetime(df['last_update'], unit='ms', utc=True)
        df['datetime'] = df['datetime'].dt.tz_convert('Etc/GMT-2')

        # filtrage des données intéressantes
        df = df[df["status"] == "OPEN"]
        df = df[["datetime", "number",	"name",	"address", "bike_stands", "available_bike_stands", "available_bikes"]]

        # on les range 
        df = df.sort_values(["datetime", "number"])

        return df

    
    except Exception as e:
        print(f"Error transforming data: {e}")

        return pd.DataFrame()

def convert_json_to_df(data):
    try:
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"Error converting JSON to DataFrame: {e}")
        return pd.DataFrame()
