import requests
import json
import pandas as pd
import pytz
import time


api_key = "ba66e04fc0c6389242d8598aeee0906fe4b3d805"
url = f"https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey={api_key}"

df_all = pd.DataFrame()
i=0

while i<60:
    # Requête à l'API
    response = requests.get(url)
    data = response.json()
    
    df = pd.DataFrame(data)
    
    df['datetime'] = pd.to_datetime(df['last_update'], unit='ms',)
    df['datetime'] = df['datetime'].dt.tz_localize('UTC').dt.tz_convert('Etc/GMT-2')
    
    df['fetch_time'] = pd.Timestamp.now()
    
    df_all = pd.concat([df_all, df], ignore_index=True)
    
    print(df[['name', 'available_bikes', 'available_bike_stands', 'datetime']].tail(5))
    
    # Attendre 1 minutes
    i+=1
    time.sleep(60)



df_all.to_csv("Extract_test_30_09_15h30.csv")