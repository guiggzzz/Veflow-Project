import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def load_to_database(airports_df, flights_df):
    """Load cleaned data into PostgreSQL database"""

    # TODO: Create connection string
    # Format: postgresql://username:password@localhost:5432/airlife_db
    connection_string = "postgresql://your_username:your_password@localhost:5432/airlife_db"

    try:
        # TODO: Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # TODO: Load airports data
        # TODO: Load flights data (only if not empty)

        print("Data loaded successfully!")

        # TODO: Print some basic statistics
        # How many airports were loaded?
        # How many flights were loaded?

    except Exception as e:
        print(f"Error loading data: {e}")

def verify_data():
    """Check that data was loaded correctly"""
    connection_string = "postgresql://your_username:your_password@localhost:5432/airlife_db"

    try:
        engine = create_engine(connection_string)

        # TODO: Query the database to verify data

        airports_count = pd.read_sql("SELECT COUNT(*) FROM airports", engine)
        print(f"Airports in database: {airports_count.iloc[0,0]}")

        flights_count = pd.read_sql("SELECT COUNT(*) FROM flights", engine) 
        print(f"Flights in database: {flights_count.iloc[0,0]}")

        # TODO: Show sample data
        sample_airports = pd.read_sql("SELECT * FROM airports LIMIT 3", engine)
        print("Sample airports:")
        print(sample_airports)

    except Exception as e:
        print(f"Error verifying data: {e}")