import pandas as pd
import requests
import clickhouse_connect
import logging
from datetime import datetime, timedelta
import numpy as np
import json
import time
import os
import subprocess
import random
from typing import Dict, List, Tuple, Optional

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/coal_mining_etl.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CoalMiningETL:    
    def __init__(self, clickhouse_host=None, clickhouse_port=None, clickhouse_user=None, clickhouse_password=None, clickhouse_db=None):
        # Use env variables with defaults
        self.clickhouse_host = clickhouse_host or os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse_port = int(clickhouse_port or os.getenv('CLICKHOUSE_PORT', '8123'))
        self.clickhouse_user = clickhouse_user or os.getenv('CLICKHOUSE_USER', 'username')
        self.clickhouse_password = clickhouse_password or os.getenv('CLICKHOUSE_PASSWORD', 'password')
        self.clickhouse_db = clickhouse_db or os.getenv('CLICKHOUSE_DB', 'coal_mining')
        
        try:
            # Connect without database first to create it
            self.client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_user,
                password=self.clickhouse_password
            )
            logger.info(f"ClickHouse connection established to {self.clickhouse_host}:{self.clickhouse_port}")
            
            # Initialize database and tables
            self.initialize_database()
            
            # Reconnect to the specific database
            self.client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_user,
                password=self.clickhouse_password,
                database=self.clickhouse_db
            )
            logger.info(f"Connected to database: {self.clickhouse_db}")
            
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

    def initialize_database(self):
        try:
            logger.info("Initializing database schema...")
            
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.clickhouse_db}")
            logger.info(f"Database {self.clickhouse_db} created/verified")
            
            self._create_tables()
            self._load_initial_data()
            
            logger.info("Database initialization completed successfully!")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise

    def _create_tables(self):
        tables = {
            'mines': """
                CREATE TABLE IF NOT EXISTS {db}.mines (
                    mine_id UInt32,
                    mine_code String,
                    mine_name String,
                    location String,
                    operational_status String
                ) ENGINE = MergeTree() ORDER BY mine_id
            """,
            'production_logs': """
                CREATE TABLE IF NOT EXISTS {db}.production_logs (
                    date Date,
                    mine_id UInt32,
                    shift String,
                    tons_extracted Float64,
                    quality_grade Float64
                ) ENGINE = MergeTree() ORDER BY (date, mine_id, shift)
            """,
            'equipment_sensors': """
                CREATE TABLE IF NOT EXISTS {db}.equipment_sensors (
                    timestamp DateTime,
                    equipment_id String,
                    status String,
                    fuel_consumption Float64,
                    maintenance_alert Bool
                ) ENGINE = MergeTree() ORDER BY (timestamp, equipment_id)
            """,
            'weather_data': """
                CREATE TABLE IF NOT EXISTS {db}.weather_data (
                    date Date,
                    temperature_mean Float64,
                    precipitation_sum Float64
                ) ENGINE = MergeTree() ORDER BY date
            """,
            'daily_production_metrics': """
                CREATE TABLE IF NOT EXISTS {db}.daily_production_metrics (
                    date Date,
                    total_production_daily Float64,
                    average_quality_grade Float64,
                    equipment_utilization Float64,
                    fuel_efficiency Float64,
                    rainfall_mm Float64,
                    weather_impact String
                ) ENGINE = MergeTree() ORDER BY date
            """
        }
        
        for table_name, create_sql in tables.items():
            self.client.command(create_sql.format(db=self.clickhouse_db))
            logger.info(f"Table {table_name} created/verified")

    def _load_initial_data(self):
        try:
            result = self.client.query(f"SELECT count() FROM {self.clickhouse_db}.production_logs")
            count = result.result_rows[0][0]
            
            if count == 0:
                logger.info("Loading initial data from production_logs.sql file...")
                
                self._load_from_sql_file()
                self._load_equipment_csv()
                
                logger.info("Initial data loaded successfully")
            else:
                logger.info(f"Production logs already contain {count} records, skipping initial data load")
                
        except Exception as e:
            logger.error(f"Failed to load initial data: {e}")
            # Don't raise - this is not critical for ETL to continue

    def _load_from_sql_file(self):
        try:
            sql_file_path = '/app/production_logs.sql'
            if os.path.exists(sql_file_path):
                with open(sql_file_path, 'r') as f:
                    sql_content = f.read()
                
                statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip() and stmt.strip().upper().startswith('INSERT')]
                
                for statement in statements:
                    if statement:
                        self.client.command(statement)
                
                logger.info(f"Executed {len(statements)} INSERT statements from production_logs.sql")
            else:
                logger.warning(f"production_logs.sql file not found at {sql_file_path}")
                
        except Exception as e:
            logger.error(f"Failed to load data from production_logs.sql: {e}")

    def _load_equipment_csv(self):
        try:
            csv_file_path = '/app/equipment_sensors.csv'
            if os.path.exists(csv_file_path):
                df = pd.read_csv(csv_file_path)
                
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df['maintenance_alert'] = df['maintenance_alert'].map({'True': True, 'False': False, True: True, False: False})
                
                batch_size = 1000
                total_rows = len(df)
                
                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i:i+batch_size]
                    data = batch.to_dict('records')
                    
                    self.client.insert(
                        f'{self.clickhouse_db}.equipment_sensors',
                        data,
                        column_names=['timestamp', 'equipment_id', 'status', 'fuel_consumption', 'maintenance_alert']
                    )
                
                logger.info(f"Loaded {total_rows} equipment sensor records from CSV")
            else:
                logger.warning(f"equipment_sensors.csv file not found at {csv_file_path}")
                
        except Exception as e:
            logger.error(f"Failed to load equipment data from CSV: {e}")

    def _generate_weather_pattern(self, date_str: str) -> Dict:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        month = date_obj.month
        
        if month in [12, 1, 2]:
            temp_base, rain_chance, max_rain = 26.0, 0.7, 25.0
        elif month in [6, 7, 8]:
            temp_base, rain_chance, max_rain = 27.0, 0.2, 8.0
        else:
            temp_base, rain_chance, max_rain = 26.5, 0.4, 15.0
        
        seed = hash(date_str) % 1000
        random.seed(seed)
        
        temperature = temp_base + random.uniform(-2, 3)
        precipitation = random.uniform(0, max_rain) if random.random() < rain_chance else 0
        
        weather_impact = 'Heavy Rain' if precipitation > 10 else 'Light Rain' if precipitation > 0 else 'Clear'
        
        return {
            'date': date_str,
            'temperature_mean': round(temperature, 1),
            'precipitation_sum': round(precipitation, 1),
            'weather_impact': weather_impact
        }

    def get_weather_data(self, date_str: str) -> Dict:
        for attempt in range(5):  # Retry up to 5 times
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
                today = datetime.now().date()
                days_diff = (today - date_obj).days

                if days_diff >= 0 and days_diff <= 92:
                    url = "https://api.open-meteo.com/v1/forecast"
                    params = {
                        'latitude': 2.0167,
                        'longitude': 117.3000,
                        'daily': 'temperature_2m_mean,precipitation_sum',
                        'timezone': 'Asia/Jakarta',
                        'past_days': min(days_diff + 1, 92),
                        'forecast_days': 0
                    }
                    
                    response = requests.get(url, params=params, timeout=15)
                    response.raise_for_status()
                    data = response.json()

                    if 'daily' in data and 'time' in data['daily'] and date_str in data['daily']['time']:
                        date_index = data['daily']['time'].index(date_str)
                        temp = data['daily']['temperature_2m_mean'][date_index]
                        precipitation = data['daily']['precipitation_sum'][date_index]
                        
                        weather_impact = 'Heavy Rain' if precipitation > 10 else 'Light Rain' if precipitation > 0 else 'Clear'
                        
                        return {
                            'date': date_str,
                            'temperature_mean': round(temp, 1),
                            'precipitation_sum': round(precipitation, 1),
                            'weather_impact': weather_impact
                        }
                
                # If date is not within API range or data not found, generate it
                return self._generate_weather_pattern(date_str)

            except (requests.exceptions.RequestException, ValueError) as e:
                logger.warning(f"Weather API attempt {attempt + 1} failed for {date_str}: {e}")
                if attempt < 4:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"Weather API failed after multiple retries for {date_str}.")
                    return self._generate_weather_pattern(date_str)
        
        return self._generate_weather_pattern(date_str)
    
    def extract_production_logs(self, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            query = """
                SELECT 
                    date,
                    mine_id,
                    shift,
                    tons_extracted,
                    quality_grade
                FROM coal_mining.production_logs
                WHERE date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date, mine_id, shift
            """.format(start_date=start_date, end_date=end_date)
            
            result = self.client.query(query)
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            logger.info(f"Extracted {len(df)} production log records")
            return df
        except Exception as e:
            logger.error(f"Error extracting production logs: {e}")
            raise
    
    def extract_equipment_sensors(self, start_date: str, end_date: str) -> pd.DataFrame:
        try:
            query = """
                SELECT 
                    timestamp,
                    equipment_id,
                    status,
                    fuel_consumption,
                    maintenance_alert
                FROM coal_mining.equipment_sensors
                WHERE toDate(timestamp) BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY timestamp, equipment_id
            """.format(start_date=start_date, end_date=end_date)
            
            result = self.client.query(query)
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            logger.info(f"Extracted {len(df)} equipment sensor records")
            return df
        except Exception as e:
            logger.error(f"Error extracting equipment sensors: {e}")
            raise
    
    def transform_data(self, production_df: pd.DataFrame, 
                      equipment_df: pd.DataFrame, 
                      weather_data: List[Dict]) -> pd.DataFrame:
        try:
            production_df['tons_extracted'] = production_df['tons_extracted'].apply(
                lambda x: max(0, x) if pd.notna(x) else 0
            )
            
            daily_production = production_df.groupby('date').agg({
                'tons_extracted': 'sum',
                'quality_grade': 'mean'
            }).reset_index()
            
            daily_production.columns = ['date', 'total_production_daily', 'average_quality_grade']
            
            equipment_df['date'] = pd.to_datetime(equipment_df['timestamp']).dt.date
            
            equipment_daily = equipment_df.groupby(['date', 'equipment_id']).agg({
                'status': lambda x: (x == 'active').mean() * 100,
                'fuel_consumption': 'mean'
            }).reset_index()
            
            equipment_summary = equipment_daily.groupby('date').agg({
                'status': 'mean',
                'fuel_consumption': 'mean'
            }).reset_index()
            
            equipment_summary.columns = ['date', 'equipment_utilization', 'avg_fuel_consumption']
            
            merged_data = daily_production.merge(equipment_summary, on='date', how='left')
            merged_data['fuel_efficiency'] = merged_data.apply(
                lambda row: row['avg_fuel_consumption'] / row['total_production_daily'] 
                if row['total_production_daily'] > 0 else 0, axis=1
            )
            
            if weather_data:
                weather_df = pd.DataFrame(weather_data)
                weather_df['date'] = pd.to_datetime(weather_df['date']).dt.date
                merged_data = merged_data.merge(weather_df, on='date', how='left')
                merged_data['rainfall_mm'] = merged_data['precipitation_sum'].fillna(0)
            else:
                merged_data['rainfall_mm'] = 0
                merged_data['temperature_mean'] = 26.0
            
            merged_data['weather_impact'] = merged_data['rainfall_mm'].apply(
                lambda x: 'High Rain' if x > 10 else 'Low Rain' if x > 0 else 'No Rain'
            )
            
            final_df = merged_data[[
                'date', 'total_production_daily', 'average_quality_grade',
                'equipment_utilization', 'fuel_efficiency', 'rainfall_mm', 'weather_impact'
            ]]
            
            logger.info(f"Transformed data for {len(final_df)} days")
            return final_df
            
        except Exception as e:
            logger.error(f"Error transforming data: {e}")
            raise
    
    def validate_data(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
        errors = []
        
        negative_production = df[df['total_production_daily'] < 0]
        if not negative_production.empty:
            errors.append(f"Found {len(negative_production)} days with negative production")
        
        invalid_utilization = df[
            (df['equipment_utilization'] < 0) | (df['equipment_utilization'] > 100)
        ]
        if not invalid_utilization.empty:
            errors.append(f"Found {len(invalid_utilization)} days with invalid equipment utilization")
        
        missing_weather = df[df['rainfall_mm'].isna()]
        if not missing_weather.empty:
            errors.append(f"Found {len(missing_weather)} days with missing weather data")
        
        invalid_quality = df[
            (df['average_quality_grade'] < 0) | (df['average_quality_grade'] > 10)
        ]
        if not invalid_quality.empty:
            errors.append(f"Found {len(invalid_quality)} days with invalid quality grades")
        
        is_valid = len(errors) == 0
        
        if errors:
            logger.warning(f"Data validation failed: {'; '.join(errors)}")
        else:
            logger.info("Data validation passed")
        
        return is_valid, errors
    
    def load_data(self, df: pd.DataFrame) -> bool:
        try:
            df['date'] = df['date'].astype(str)
            
            dates = df['date'].tolist()
            if dates:
                date_list = "', '".join(dates)
                delete_query = f"ALTER TABLE coal_mining.daily_production_metrics DELETE WHERE date IN ('{date_list}')"
                self.client.command(delete_query)
                logger.info(f"Cleared existing data for {len(dates)} dates")
            
            self.client.insert(
                'coal_mining.daily_production_metrics',
                df.to_dict('records'),
                column_names=list(df.columns)
            )
            
            logger.info(f"Successfully loaded {len(df)} records to daily_production_metrics")
            return True
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def run_etl_pipeline(self, start_date: str, end_date: str) -> bool:
        try:
            logger.info(f"Starting ETL pipeline for {start_date} to {end_date}")
            
            production_df = self.extract_production_logs(start_date, end_date)
            equipment_df = self.extract_equipment_sensors(start_date, end_date)
            
            weather_data = []
            current_date = datetime.strptime(start_date, '%Y-%m-%d')
            end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            while current_date <= end_date_dt:
                date_str = current_date.strftime('%Y-%m-%d')
                weather = self.get_weather_data(date_str)
                if weather:
                    weather_data.append(weather)
                current_date += timedelta(days=1)
                time.sleep(0.1)
            
            transformed_df = self.transform_data(production_df, equipment_df, weather_data)
            is_valid, errors = self.validate_data(transformed_df)
            
            if not is_valid:
                with open('data_validation_errors.log', 'a') as f:
                    f.write(f"{datetime.now()}: {'; '.join(errors)}\n")
            
            success = self.load_data(transformed_df)
            
            if success:
                logger.info("ETL pipeline completed successfully")
                return True
            else:
                logger.error("ETL pipeline failed during data loading")
                return False
                
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            return False

    def populate_daily_metrics(self) -> bool:
        try:
            logger.info("Populating daily_production_metrics with weather data...")
            
            self.client.command("TRUNCATE TABLE coal_mining.daily_production_metrics")
            
            production_query = """
                SELECT 
                    date,
                    SUM(tons_extracted) as total_production_daily,
                    AVG(quality_grade) as average_quality_grade,
                    COUNT(*) as shifts_count
                FROM coal_mining.production_logs 
                WHERE tons_extracted > 0 
                GROUP BY date 
                ORDER BY date
            """
            
            production_result = self.client.query(production_query)
            production_data = production_result.result_rows
            
            logger.info(f"Found {len(production_data)} days of production data")
            
            try:
                equipment_query = """
                    SELECT 
                        toDate(timestamp) as date,
                        AVG(CASE WHEN status = 'active' THEN 1 ELSE 0 END) * 100 as equipment_utilization,
                        AVG(fuel_consumption) as avg_fuel_consumption
                    FROM coal_mining.equipment_sensors 
                    GROUP BY date 
                    ORDER BY date
                """
                equipment_result = self.client.query(equipment_query)
                equipment_dict = {row[0]: {'utilization': row[1], 'fuel': row[2]} 
                                for row in equipment_result.result_rows}
            except Exception as eq_error:
                logger.warning(f"Equipment query failed: {eq_error}")
                equipment_dict = {}
            
            final_data = []
            
            for i, (date, total_prod, avg_quality, shifts) in enumerate(production_data):
                try:
                    date_str = date.strftime('%Y-%m-%d')
                    
                    # Debug logging for problematic values
                    logger.debug(f"Processing day {i+1}: date={date}, total_prod={total_prod} (type: {type(total_prod)}), avg_quality={avg_quality} (type: {type(avg_quality)})")
                    
                    weather = self.get_weather_data(date_str)
                    equipment = equipment_dict.get(date, {'utilization': 85.0, 'fuel': 20.0})
                    
                    # More robust None handling with explicit type conversion
                    fuel = float(equipment.get('fuel', 0.0)) if equipment.get('fuel') is not None else 0.0
                    utilization = float(equipment.get('utilization', 85.0)) if equipment.get('utilization') is not None else 85.0
                    prod = float(total_prod) if total_prod is not None else 0.0
                    quality = float(avg_quality) if avg_quality is not None else 0.0
                    
                    # Calculate fuel efficiency with extra safety
                    try:
                        if prod > 0 and fuel > 0:
                            fuel_efficiency = (fuel / prod) * 1000
                        else:
                            fuel_efficiency = 2.5
                    except (TypeError, ZeroDivisionError) as calc_error:
                        logger.warning(f"Fuel efficiency calculation failed for {date_str}: {calc_error}, using default")
                        fuel_efficiency = 2.5
                    
                    final_record = {
                        'date': date,
                        'total_production_daily': prod,
                        'average_quality_grade': quality,
                        'equipment_utilization': utilization,
                        'fuel_efficiency': fuel_efficiency,
                        'rainfall_mm': weather['precipitation_sum'],
                        'weather_impact': weather['weather_impact']
                    }
                    
                    final_data.append(final_record)
                    
                    time.sleep(0.1)
                    if (i + 1) % 10 == 0:
                        logger.info(f"Processed {i + 1}/{len(production_data)} days...")
                        
                except Exception as row_error:
                    logger.error(f"Error processing row {i+1} (date: {date}): {row_error}")
                    logger.error(f"Row data: date={date}, total_prod={total_prod}, avg_quality={avg_quality}, shifts={shifts}")
                    # Continue processing other rows instead of failing completely
                    continue
            
            self.client.insert(
                'coal_mining.daily_production_metrics',
                final_data,
                column_names=['date', 'total_production_daily', 'average_quality_grade', 
                             'equipment_utilization', 'fuel_efficiency', 'rainfall_mm', 'weather_impact']
            )
            
            logger.info(f"Successfully populated {len(final_data)} records with weather data!")
            return True
            
        except Exception as e:
            logger.error(f"Failed to populate daily metrics: {e}")
            import traceback
            logger.error(f"Full traceback: {traceback.format_exc()}")
            return False

def main():
    try:
        etl = CoalMiningETL()
        
        logger.info("Populating daily production metrics with weather data...")
        success = etl.populate_daily_metrics()
        
        if success:
            logger.info("Daily metrics populated successfully!")
            print("Coal Mining ETL Pipeline completed successfully!")
        else:
            logger.error("Failed to populate daily metrics")
            print("Coal Mining ETL Pipeline failed!")
            exit(1)
            
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
