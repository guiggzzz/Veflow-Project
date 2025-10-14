"""
Data Loading Module

This module handles loading cleaned data into PostgreSQL database:
- Load data from api to table 
- Verify data was loaded correctly
"""

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2

# Database connection configuration
# TODO: Update these values with your actual database credentials
DATABASE_CONFIG = {
    'username': 'mqtho',
    'password': 'Thomas0911mqtho', 
    'host': 'localhost',
    'port': '5432',
    'database': 'veflow_db'
}

def get_connection_string():
    """Build PostgreSQL connection string"""
    return f"postgresql://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['database']}"

def load_to_database(veflow_df):
    """
    Load cleaned data into PostgreSQL database
    
    Args:
        veflow_df: cleaned training dataset for veflow
    """
    print("💾 Loading data to PostgreSQL database...")
    
    # TODO: Create connection string using the function above
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        engine = create_engine(connection_string)
    
        # TODO: Load veflow data
        if not veflow_df.empty:
            veflow_df.to_sql('veflow', engine, if_exists='append', index=False)
        
        # TODO: Print loading statistics
        if not veflow_df.empty:
            print(f"✅ Loaded {len(veflow_df)} observations to database")
        else:
            print("ℹ️  No veflow data to load")
        
    except Exception as e:
        print(f"❌ Error loading data to database: {e}")
        print("💡 Make sure:")
        print("   - PostgreSQL is running")
        print("   - Database 'veflow_db' exists") 
        print("   - Username and password are correct")
        print("   - Tables are created (run database_setup.sql)")

def verify_data():
    """
    Verify that data was loaded correctly by running some basic queries
    """
    print("🔍 Verifying data was loaded correctly...")
    
    connection_string = get_connection_string()
    
    try:
        # TODO: Create SQLAlchemy engine
        engine = create_engine(connection_string)
        
        # print("⚠️  Data verification not yet implemented")
        # return
        
        veflow_count = pd.read_sql("SELECT COUNT(*) as count FROM veflow", engine)
        print(f"📊 Veflow in database: {veflow_count.iloc[0]['count']}")
        
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
            
            if len(tables) == 1:
                print("✅ Required tables (veflow) exist")
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
