import requests
import json
import pandas as pd
import time

api_key = "ba66e04fc0c6389242d8598aeee0906fe4b3d805"
url = f"https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey={api_key}"

df_all = pd.DataFrame()
i=0

while i<20:
    response = requests.get(url)
    data = response.json()
    df = pd.json_normalize(data)

    df['datetime'] = pd.to_datetime(df['last_update'], unit='ms', utc=True)
    df['datetime'] = df['datetime'].dt.tz_convert('Etc/GMT-2')
    
    df['fetch_time'] = pd.Timestamp.now()
    
    df_all = pd.concat([df_all, df], ignore_index=True)
    
    print(df[['name', 'available_bikes', 'available_bike_stands', 'datetime']].tail(5))
    
    i+=1
    time.sleep(60)


df_all.to_csv("Extract_test_30_09_16h00.csv")




