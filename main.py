from extract_data import extract
from transform_data import transform
from load_data import test_database_connection, load_to_database, verify_data
import json


def main():
    # Configuration
    API_KEY = "ba66e04fc0c6389242d8598aeee0906fe4b3d805"
    BASE_URL = f"https://api.jcdecaux.com/vls/v1/stations?contract=toulouse&apiKey={API_KEY}"
    
    # Extraction
    print("Démarrage de l'ETL\n")
    print("Extraction des données depuis l'API\n")
    data = extract(BASE_URL, API_KEY)

    # Transformation
    print("Transformation des données  extraites depuis l'API\n")
    df = transform(data)

    test_database_connection()
    load_to_database(df)
    verify_data()


    #optionnel, juste pour les tests on print les data
    # print(f"\nAperçu des données:\n")
    # print(df.head())


if __name__ == "__main__":
    main()