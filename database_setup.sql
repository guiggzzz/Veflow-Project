-- VeFlow Database Setup
-- Run this script to create the necessary tables for the ETL pipeline

-- Drop tables if they exist (for clean restart)
DROP TABLE IF EXISTS velostoulouse;
DROP TABLE IF EXISTS stationtoulouse;

-- VeloToulouse table - stores velos distribution information from CSV data
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

-- Station table - stores station informations of Toulouse
CREATE TABLE stationtoulouse (
    id SERIAL PRIMARY KEY,
    name_station VARCHAR(100),
    address_station VARCHAR(10),
    longitude DECIMAL(10,6),
    latitude DECIMAL(10,6)
);

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

-- Create indexes for better query performance
CREATE INDEX idx_station_name_velos ON velostoulouse(name_station);
CREATE INDEX idx_station_name_stations ON stationtoulouse(name_station);

-- Verify tables were created
\dt

SELECT 'Database setup complete!' as status;