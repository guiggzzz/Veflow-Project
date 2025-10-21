from ETL.extract_data import extract_status, read_stations_toulouse
from ETL.transform_data import transform_stations_data, transform_status
from ETL.load_data import test_database_connection, load_to_database, verify_data, load_json_to_database
import json


def main():

    # Configuration
    API_KEY = "ba66e04fc0c6389242d8598aeee0906fe4b3d805"
    BASE_URL = f"https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey={API_KEY}"



    # Load station data from local JSON file
    station_data = read_stations_toulouse("./data/toulouse.json")
    station_df = transform_stations_data(station_data)
    load_json_to_database(station_df)

    # On appelle l'API JCDecaux pour extraire les données en temps réel des stations.
    
    # Extraction
    print("Démarrage de l'ETL\n")
    print("Extraction des données depuis l'API\n")
    data = extract_status(BASE_URL, API_KEY)

    # Transformation
    print("Transformation des données  extraites depuis l'API\n")
    df = transform_status(data)

    test_database_connection()
    load_to_database(df)
    verify_data()


if __name__ == "__main__":
    main()