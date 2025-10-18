import psycopg2
from psycopg2 import sql

# paramètres de connexion à Postgres (instance principale, pas la DB à créer)
conn = psycopg2.connect(
    dbname="postgres",   # DB par défaut
    user="mqtho",     # ton utilisateur
    password="Thomas0911mqtho", # ton mot de passe
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
    conn = psycopg2.connect(dbname=db_name, user="mqtho", password="Thomas0911mqtho", host="localhost", port="5432")
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS velostoulouse;")
    cur.execute("""
        CREATE TABLE velostoulouse (
            id SERIAL PRIMARY KEY,
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            id_station INTEGER,
            name_station VARCHAR(100),
            bike_stands INTEGER,
            available_bike_stands INTEGER,
            available_bikes INTEGER,
            open_status BOOLEAN
        );
    """)
    print("Table velostoulouse created (if it did not exist).")

    cur.execute("DROP TABLE IF EXISTS stationtoulouse;")
    cur.execute("""
        CREATE TABLE stationtoulouse (
        id SERIAL PRIMARY KEY,
        name_station VARCHAR(100),
        address_station VARCHAR(10),
        longitude DECIMAL(10,6),
        latitude DECIMAL(10,6)
    );
    """)
    print("Table stationtoulouse created (if it did not exist).")

    cur.execute("DROP TABLE IF EXISTS veflow;")
    cur.execute("""
        CREATE TABLE veflow (
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
    print("Table veflow created (if it did not exist).")

    conn.commit()
    cur.close()
    conn.close()

else:
    print(f"Database {db_name} already exists.")

    cur.close()
    conn.close()