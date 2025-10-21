import psycopg2
from psycopg2 import sql


USER = "mqtho"
PASSWORD = ""

# paramètres de connexion à Postgres (instance principale, pas la DB à créer)
conn = psycopg2.connect(
    dbname="postgres",   # DB par défaut
    user=USER,     # ton utilisateur
    password=PASSWORD, # ton mot de passe
    host="localhost",
    port="5432"
)
conn.autocommit = True  # nécessaire pour CREATE DATABASE
cur = conn.cursor()

# 1. Créer une nouvelle base de données
db_name = "veflow_db"

# Check if database exists
cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
exists = cur.fetchone()

if not exists:
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    print(f"Database {db_name} created.")

    # create the table
    conn = psycopg2.connect(dbname=db_name, user=USER, password=PASSWORD, host="localhost", port="5432")
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS station_toulouse;")
    cur.execute("""
        CREATE TABLE station_toulouse (
        id SERIAL PRIMARY KEY,
        name_station VARCHAR,
        address_station VARCHAR,
        longitude DECIMAL(10,6),
        latitude DECIMAL(10,6)
    );
    """)
    print("Table station_toulouse created (if it did not exist).")

    cur.execute("DROP TABLE IF EXISTS statut_stations;")
    cur.execute("""
        CREATE TABLE statut_stations (
        datetime TIMESTAMP NOT NULL,
        number INTEGER,
        name VARCHAR(100),
        address VARCHAR(255),
        bike_stands INTEGER,
        available_bike_stands INTEGER,
        available_bikes INTEGER,
        UNIQUE (datetime, number)
    );
    """)
    print("Table statut_stations created (if it did not exist).")

    conn.commit()
    cur.close()
    conn.close()

else:
    print(f"Database {db_name} already exists.")

    cur.close()
    conn.close()