"""
Data Loading Module

This module handles loading cleaned data into PostgreSQL database:
- Load data from api to table 
- Verify data was loaded correctly
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import psycopg2
import os

USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Database connection configuration
# TODO: Update these values with your actual database credentials
DATABASE_CONFIG = {
    'username': USER,
    'password': PASSWORD,
    'host': 'localhost',
    'port': '5432',
    'database': 'veflow_db'
}

def get_connection_string():
    """Build PostgreSQL connection string"""
    return f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

def load_to_database(status_df):
    """
    Load cleaned data into PostgreSQL database
    
    Args:
        status_df: cleaned training dataset for veflow
    """
    print("💾 Loading data to PostgreSQL database...")
    
    # TODO: Create connection string using the function above
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        engine = create_engine(connection_string)
    
        # TODO: Load status data
        if not status_df.empty:
            rows = status_df.to_dict(orient='records')
            insert_query = text("""
                INSERT INTO status_stations (datetime, number, name, address, bike_stands, available_bike_stands, available_bikes)
                VALUES (:datetime, :number, :name, :address, :bike_stands, :available_bike_stands, :available_bikes)
                ON CONFLICT DO NOTHING
            """)

            with engine.begin() as conn:
                result = conn.execute(insert_query, rows)
                inserted_rows = result.rowcount  # number of rows actually inserted

            print(f"✅ Loaded {inserted_rows} / {len(status_df)} observations to database")
        else:
            print("ℹ️  No status data to load")

    except Exception as e:
        print(f"❌ Error loading data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'veflow_db' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run setup_db.py)")

def load_json_to_database(df):
    """
    Load cleaned data into PostgreSQL database
    
    Args:
        df: cleaned training dataset for stations_toulouse
    """
    print("💾 Loading data to PostgreSQL database...")
    
    # TODO: Create connection string using the function above
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        engine = create_engine(connection_string)
    
        # TODO: Load stations_toulouse data
        if not df.empty:
            df.to_sql('stations_toulouse', engine, if_exists='replace', index=False)

            print(f"✅ Loaded {len(df)} observations to database")
        else:
            print("ℹ️  No stations_toulouse data to load")

    except Exception as e:
        print(f"❌ Error loading data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'veflow_db' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run setup_db.py)")

def verify_data():
    """
    Verify that data was loaded correctly by running some basic queries
    """
    print("🔍 Verifying data was loaded correctly...")
    
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        engine = create_engine(connection_string)

        stations_count = pd.read_sql("SELECT COUNT(*) as count FROM stations_toulouse", engine)
        print(f"📊 Stations in database: {stations_count.iloc[0]['count']}")

        recent_station = pd.read_sql("SELECT * FROM stations_toulouse ORDER BY id DESC LIMIT 5", engine)
        print(f"📋 Station entries:\n{recent_station}")

        status_count = pd.read_sql("SELECT COUNT(*) as count FROM status_stations", engine)
        print(f"📊 Status in database: {status_count.iloc[0]['count']}")

        recent_status = pd.read_sql("SELECT * FROM status_stations ORDER BY datetime DESC LIMIT 5", engine)
        print(f"📋 5 most recent status entries:\n{recent_status}")
        
    except Exception as e:
        print(f"❌ Error verifying data: {e}")


def test_database_connection():
    """
    Test database connection without loading data
    Students can use this to debug connection issues
    """
    print("🔌 Testing database connection...")
    
    connection_string = get_connection_string()
    
    try:
        engine = create_engine(connection_string)
        
        # Try a simple query
        result = pd.read_sql("SELECT 1 as test", engine)
        
        if result.iloc[0]['test'] == 1:
            print("✅ Database connection successful!")
            
            # Check if our tables exist
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('veflow')
            ORDER BY table_name
            """
            tables = pd.read_sql(tables_query, engine)
            
            if len(tables) == 2:
                print("✅ Required tables (stations_toulouse, status_stations) exist")
            else:
                print(f"⚠️  Found {len(tables)} tables, expected 2")
                print("💡 Run database_setup.sql to create tables")
            
            return True
        else:
            print("❌ Database connection test failed")
            return False
            
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Check your connection settings in DATABASE_CONFIG")
        return False
