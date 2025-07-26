#!/bin/bash

echo "Starting  ETL Pipeline..."

# Wait for ClickHouse to be ready
echo "Waiting for ClickHouse to be ready..."
while ! curl -s "http://clickhouse:8123/?user=username&password=password&query=SELECT%201" > /dev/null; do
    echo "Waiting for ClickHouse..."
    sleep 5
done

echo "ClickHouse is ready!"

echo "Initializing database schema..."

execute_query() {
    curl -s "http://clickhouse:8123/?user=username&password=password" \
         -d "$1" || echo "Query failed: $1"
}

# Create database
execute_query "CREATE DATABASE IF NOT EXISTS coal_mining"

# Create tables
echo "Creating tables..."

execute_query "
CREATE TABLE IF NOT EXISTS coal_mining.mines (
    mine_id UInt32,
    mine_code String,
    mine_name String,
    location String,
    operational_status String
)
ENGINE = MergeTree()
ORDER BY mine_id
"

execute_query "
CREATE TABLE IF NOT EXISTS coal_mining.production_logs (
    date Date,
    mine_id UInt32,
    shift String,
    tons_extracted Float64,
    quality_grade Float64
)
ENGINE = MergeTree()
ORDER BY (date, mine_id, shift)
"

execute_query "
CREATE TABLE IF NOT EXISTS coal_mining.equipment_sensors (
    timestamp DateTime,
    equipment_id String,
    status String,
    fuel_consumption Float64,
    maintenance_alert Bool
)
ENGINE = MergeTree()
ORDER BY (timestamp, equipment_id)
"

execute_query "
CREATE TABLE IF NOT EXISTS coal_mining.weather_data (
    date Date,
    temperature_mean Float64,
    precipitation_sum Float64
)
ENGINE = MergeTree()
ORDER BY date
"

execute_query "
CREATE TABLE IF NOT EXISTS coal_mining.daily_production_metrics (
    date Date,
    total_production_daily Float64,
    average_quality_grade Float64,
    equipment_utilization Float64,
    fuel_efficiency Float64,
    rainfall_mm Float64,
    weather_impact String
)
ENGINE = MergeTree()
ORDER BY date
"

# Load data from production_logs.sql if available
if [ -f "/app/production_logs.sql" ]; then
    echo "Loading data from production_logs.sql..."
    # Execute production_logs.sql statements
    cat /app/production_logs.sql | while read -r line; do
        if [[ $line == INSERT* ]]; then
            execute_query "$line"
        fi
    done
    echo "Data from production_logs.sql loaded successfully!"
else
    echo "production_logs.sql not found, loading basic mine data..."
    execute_query "
    INSERT INTO coal_mining.mines VALUES 
    (1, 'MINE001', 'Bukit Bara', 'Berau, Kalimantan', 'Active'),
    (2, 'MINE002', 'Gunung Hitam', 'Berau, Kalimantan', 'Active'),
    (3, 'MINE003', 'Sumber Jaya', 'Berau, Kalimantan', 'Maintenance')
    "
fi

echo "Database init completed!"

# Run the main ETL pipeline
echo "Running ETL pipeline..."
python /app/coal_mining_etl.py

if [ $? -eq 0 ]; then
    echo "ETL pipeline completed successfully!"
    
    # Run prediction model if ETL succeeded
    echo "Running prediction model..."
    python /app/prediction_model.py
    
    if [ $? -eq 0 ]; then
        echo "Prediction model completed successfully!"
    else
        echo "Prediction model failed!"
    fi
else
    echo "ETL pipeline failed!"
    exit 1
fi

echo "Coal Mining data pipeline completed!"

# Keep container running
echo "ETL service is ready. Keeping container alive..."
tail -f /dev/null
