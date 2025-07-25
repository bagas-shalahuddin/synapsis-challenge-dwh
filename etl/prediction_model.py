#!/usr/bin/env python3
"""
Coal Mining Production Prediction Model
PT Synapsis Sinergi Digital - AI Engineer Challenge (Bonus)

This script implements a time series forecasting model to predict next day production data.
Uses historical production data, weather data, and equipment utilization metrics.
"""

import pandas as pd
import numpy as np
import clickhouse_connect
import logging
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import warnings
import os
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ProductionPredictor:
    """Time series forecasting model for coal production prediction"""
    
    def __init__(self, clickhouse_host=None, clickhouse_port=None,
                 clickhouse_user=None, clickhouse_password=None, clickhouse_db=None):
        """Initialize ClickHouse connection with environment variable fallbacks"""
        # Use environment variables with defaults
        self.clickhouse_host = clickhouse_host or os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.clickhouse_port = int(clickhouse_port or os.getenv('CLICKHOUSE_PORT', '8123'))
        self.clickhouse_user = clickhouse_user or os.getenv('CLICKHOUSE_USER', 'username')
        self.clickhouse_password = clickhouse_password or os.getenv('CLICKHOUSE_PASSWORD', 'password')
        self.clickhouse_db = clickhouse_db or os.getenv('CLICKHOUSE_DB', 'coal_mining')
        
        try:
            self.client = clickhouse_connect.get_client(
                host=self.clickhouse_host,
                port=self.clickhouse_port,
                username=self.clickhouse_user,
                password=self.clickhouse_password,
                database=self.clickhouse_db
            )
            self.model = None
            logger.info(f"ClickHouse connection established for prediction model to {self.clickhouse_host}:{self.clickhouse_port}")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def extract_historical_data(self, days_back=90):
        """Extract historical production metrics for training"""
        try:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            query = """
                SELECT 
                    date,
                    total_production_daily,
                    average_quality_grade,
                    equipment_utilization,
                    fuel_efficiency,
                    rainfall_mm
                FROM coal_mining.daily_production_metrics
                WHERE date BETWEEN '{start_date}' AND '{end_date}'
                ORDER BY date
            """.format(start_date=start_date, end_date=end_date)
            
            result = self.client.query(query)
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            df['date'] = pd.to_datetime(df['date'])
            
            logger.info(f"Extracted {len(df)} historical records for training")
            return df
        except Exception as e:
            logger.error(f"Error extracting historical data: {e}")
            raise
    
    def create_features(self, df):
        """Create features for time series prediction"""
        try:
            # Sort by date
            df = df.sort_values('date').reset_index(drop=True)
            
            # Create lag features (previous days)
            for lag in [1, 2, 3, 7]:
                df[f'production_lag_{lag}'] = df['total_production_daily'].shift(lag)
                df[f'quality_lag_{lag}'] = df['average_quality_grade'].shift(lag)
                df[f'utilization_lag_{lag}'] = df['equipment_utilization'].shift(lag)
            
            # Create rolling averages
            for window in [3, 7, 14]:
                df[f'production_rolling_{window}'] = df['total_production_daily'].rolling(window=window).mean()
                df[f'quality_rolling_{window}'] = df['average_quality_grade'].rolling(window=window).mean()
                df[f'utilization_rolling_{window}'] = df['equipment_utilization'].rolling(window=window).mean()
            
            # Time-based features
            df['day_of_week'] = df['date'].dt.dayofweek
            df['day_of_month'] = df['date'].dt.day
            df['month'] = df['date'].dt.month
            
            # Weather impact features
            df['is_rainy'] = (df['rainfall_mm'] > 0).astype(int)
            df['heavy_rain'] = (df['rainfall_mm'] > 10).astype(int)
            
            # Drop rows with NaN values (due to lag features)
            df = df.dropna().reset_index(drop=True)
            
            logger.info(f"Created features, remaining {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error creating features: {e}")
            raise
    
    def prepare_training_data(self, df):
        """Prepare features and target for training"""
        try:
            # Feature columns (excluding date and target)
            feature_cols = [col for col in df.columns if col not in ['date', 'total_production_daily']]
            
            X = df[feature_cols]
            y = df['total_production_daily']
            
            logger.info(f"Prepared training data: {X.shape[0]} samples, {X.shape[1]} features")
            return X, y, feature_cols
        except Exception as e:
            logger.error(f"Error preparing training data: {e}")
            raise
    
    def train_model(self, X, y):
        """Train the prediction model"""
        try:
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42, shuffle=False
            )
            
            # Train Random Forest model
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                n_jobs=-1
            )
            
            self.model.fit(X_train, y_train)
            
            # Evaluate model
            y_pred = self.model.predict(X_test)
            
            mae = mean_absolute_error(y_test, y_pred)
            rmse = np.sqrt(mean_squared_error(y_test, y_pred))
            r2 = r2_score(y_test, y_pred)
            
            logger.info(f"Model Performance:")
            logger.info(f"MAE: {mae:.2f}")
            logger.info(f"RMSE: {rmse:.2f}")
            logger.info(f"R²: {r2:.3f}")
            
            # Feature importance
            feature_importance = pd.DataFrame({
                'feature': X.columns,
                'importance': self.model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            logger.info("Top 10 Most Important Features:")
            for _, row in feature_importance.head(10).iterrows():
                logger.info(f"  {row['feature']}: {row['importance']:.3f}")
            
            return {
                'mae': mae,
                'rmse': rmse,
                'r2': r2,
                'feature_importance': feature_importance
            }
        except Exception as e:
            logger.error(f"Error training model: {e}")
            raise
    
    def predict_next_day(self, feature_cols):
        """Predict production for the next day"""
        try:
            if self.model is None:
                raise ValueError("Model not trained yet")
            
            # Get the latest data for prediction
            query = """
                SELECT 
                    date,
                    total_production_daily,
                    average_quality_grade,
                    equipment_utilization,
                    fuel_efficiency,
                    rainfall_mm
                FROM coal_mining.daily_production_metrics
                ORDER BY date DESC
                LIMIT 30
            """
            
            result = self.client.query(query)
            df = pd.DataFrame(result.result_rows, columns=result.column_names)
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date').reset_index(drop=True)
            
            # Create features for the latest data
            df_with_features = self.create_features(df)
            
            if len(df_with_features) == 0:
                raise ValueError("Not enough data to create features for prediction")
            
            # Get the latest record for prediction
            latest_features = df_with_features[feature_cols].iloc[-1:].fillna(0)
            
            # Make prediction
            prediction = self.model.predict(latest_features)[0]
            
            next_date = (df['date'].max() + timedelta(days=1)).strftime('%Y-%m-%d')
            
            logger.info(f"Predicted production for {next_date}: {prediction:.2f} tons")
            
            return {
                'date': next_date,
                'predicted_production': prediction,
                'confidence': 'Medium'  # Could be improved with prediction intervals
            }
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            raise
    
    def save_model(self, filepath='coal_production_model.pkl'):
        """Save the trained model"""
        try:
            if self.model is None:
                raise ValueError("No model to save")
            
            joblib.dump(self.model, filepath)
            logger.info(f"Model saved to {filepath}")
        except Exception as e:
            logger.error(f"Error saving model: {e}")
            raise
    
    def load_model(self, filepath='coal_production_model.pkl'):
        """Load a trained model"""
        try:
            self.model = joblib.load(filepath)
            logger.info(f"Model loaded from {filepath}")
        except Exception as e:
            logger.error(f"Error loading model: {e}")
            raise
    
    def run_prediction_pipeline(self):
        """Run the complete prediction pipeline"""
        try:
            logger.info("Starting prediction pipeline")
            
            # Extract historical data
            df = self.extract_historical_data(days_back=90)
            
            if len(df) < 30:
                logger.warning("Insufficient historical data for training")
                return None
            
            # Create features
            df_with_features = self.create_features(df)
            
            # Prepare training data
            X, y, feature_cols = self.prepare_training_data(df_with_features)
            
            # Train model
            performance = self.train_model(X, y)
            
            # Make prediction for next day
            prediction = self.predict_next_day(feature_cols)
            
            # Save model
            self.save_model()
            
            logger.info("Prediction pipeline completed successfully")
            
            return {
                'model_performance': performance,
                'next_day_prediction': prediction
            }
        except Exception as e:
            logger.error(f"Prediction pipeline failed: {e}")
            return None

def main():
    """Main execution function"""
    try:
        # Initialize predictor with environment variables
        predictor = ProductionPredictor()
        
        # Run prediction pipeline
        results = predictor.run_prediction_pipeline()
        
        if results:
            print("Coal Mining Production Prediction completed successfully!")
            print(f"Next day prediction: {results['next_day_prediction']}")
            print(f"Model R²: {results['model_performance']['r2']:.3f}")
        else:
            print("Prediction pipeline failed!")
            exit(1)
            
    except Exception as e:
        logger.error(f"Main execution failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
