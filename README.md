# Coal Mining Data Pipeline

A comprehensive data engineering solution for optimizing coal mining operations through real-time data collection, transformation, and visualization. This project implements an end-to-end ETL pipeline that processes production data, IoT sensor data, and weather information to generate actionable insights for mining operations.

## Project Overview

This data pipeline solution addresses the challenge of integrating multiple data sources in coal mining operations to optimize production efficiency, monitor equipment performance, and analyze environmental factors affecting mining output.

### Key Features

- **Multi-source data integration**: SQL database, CSV files, and REST API
- **Real-time ETL processing**: Automated data extraction, transformation, and loading
- **Data quality validation**: Comprehensive anomaly detection and data cleansing
- **Interactive dashboards**: Visual analytics for production metrics and trends
- **Predictive analytics**: Time series forecasting for production planning
- **Containerized deployment**: Docker-based infrastructure for easy deployment and scaling

## Architecture

The solution consists of the following components:

- **ClickHouse**: High-performance analytical database for data warehousing
- **Metabase**: Business intelligence platform for dashboard visualization
- **PostgreSQL**: Metadata storage for Metabase configuration
- **Python ETL Service**: Custom data processing pipeline
- **Docker Compose**: Container orchestration for the entire stack

## Data Sources

### 1. Production Database
- **Source**: SQL table `production_logs`
- **Schema**: `date`, `mine_id`, `shift`, `tons_extracted`, `quality_grade`
- **Purpose**: Core production metrics and mining output data

### 2. IoT Sensor Data
- **Source**: CSV file `equipment_sensors.csv`
- **Schema**: `timestamp`, `equipment_id`, `status`, `fuel_consumption`, `maintenance_alert`
- **Purpose**: Equipment performance monitoring and operational status

### 3. Weather API
- **Source**: Open-Meteo API for Berau, Kalimantan, Indonesia
- **Coordinates**: 2.0167°N, 117.3000°E
- **Data**: Daily temperature and precipitation measurements
- **Purpose**: Environmental impact analysis on production

## Generated Metrics

The ETL pipeline transforms raw data into the following key performance indicators:

| Metric | Description | Calculation |
|--------|-------------|-------------|
| `total_production_daily` | Total tons of coal mined per day | Sum of daily production across all mines |
| `average_quality_grade` | Average coal quality per day | Mean quality grade weighted by production volume |
| `equipment_utilization` | Equipment operational efficiency | Percentage of time equipment status is "active" |
| `fuel_efficiency` | Fuel consumption per production unit | Average fuel consumption per ton of coal mined |
| `weather_impact` | Correlation between weather and production | Statistical analysis of rainfall vs production output |

## Quick Start

### Prerequisites

- Docker and Docker Compose
- 4GB+ available RAM
- Network access for weather API calls

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd metabase-clickhouse
   ```

2. **Start the services**
   ```bash
   docker-compose up -d
   ```

3. **Verify deployment**
   ```bash
   docker-compose ps
   ```

### Service Access

- **Metabase Dashboard**: http://localhost:3000
- **ClickHouse Database**: http://localhost:8123
- **PostgreSQL**: localhost:5432

### Initial Setup

1. **Access Metabase**: Navigate to http://localhost:3000
2. **Complete setup wizard**: Create admin account and configure database connections
3. **Connect to ClickHouse**: Use the provided connection parameters
4. **Import dashboards**: Load pre-configured visualization templates

## Project Structure

```
metabase-clickhouse/
├── docker-compose.yml              # Container orchestration configuration
├── data-origin/                    # Source data files
│   ├── equipment_sensors.csv       # IoT sensor data
│   └── init.sql                    # Database initialization script
├── etl/                           # ETL pipeline components
│   ├── coal_mining_etl.py         # Main ETL processing script
│   ├── prediction_model.py        # Time series forecasting model
│   ├── Dockerfile                 # ETL service container definition
│   ├── requirements.txt           # Python dependencies
│   ├── run_etl.sh                # ETL execution script
│   └── logs/                     # ETL processing logs
├── metabase-dockerfile/           # Metabase configuration
│   └── plugins/                  # Database drivers and extensions
├── docs/                         # Documentation and queries
│   └── dashboard_queries.sql     # Pre-built dashboard queries
└── README.md                     # Project documentation
```

## Data Pipeline Process

### 1. Data Extraction

- **Production Data**: Direct SQL queries to production database
- **Sensor Data**: Batch processing of CSV files with timestamp-based filtering
- **Weather Data**: RESTful API calls with automatic retry logic and error handling

### 2. Data Transformation

- **Data Cleansing**: Removal of invalid records and outliers
- **Metric Calculation**: Aggregation and statistical analysis
- **Data Enrichment**: Weather correlation and production impact analysis
- **Quality Validation**: Comprehensive data quality checks and anomaly detection

### 3. Data Loading

- **Target Schema**: Optimized ClickHouse tables for analytical queries
- **Batch Processing**: Efficient bulk loading with transaction management
- **Incremental Updates**: Delta processing for ongoing data synchronization

## Dashboard Visualizations

### 1. Production Trends (Line Chart)
- **Purpose**: Track daily production patterns over time
- **Metrics**: Total production volume with trend analysis
- **Time Range**: Configurable date range selection

### 2. Mine Performance Comparison (Bar Chart)
- **Purpose**: Compare production efficiency across mining locations
- **Metrics**: Average quality grade and total production by mine
- **Insights**: Identify top-performing and underperforming locations

### 3. Weather Impact Analysis (Scatter Plot)
- **Purpose**: Analyze correlation between weather conditions and production
- **Metrics**: Rainfall vs production output correlation
- **Applications**: Weather-based production planning and risk assessment

## Data Quality Validation

### Validation Rules

- **Production Validation**: Ensure `total_production_daily` is non-negative
- **Equipment Validation**: Verify `equipment_utilization` is between 0-100%
- **Weather Validation**: Confirm complete weather data for all production days
- **Anomaly Detection**: Flag unusual patterns and outliers for investigation

### Error Handling

- **Logging**: Comprehensive error logging with severity levels
- **Alerting**: Automated notifications for critical data quality issues
- **Recovery**: Automatic retry mechanisms and fallback procedures

## Predictive Analytics

### Time Series Forecasting

The project includes a machine learning component for production prediction:

- **Model Type**: ARIMA-based time series forecasting
- **Prediction Horizon**: Next-day production estimates
- **Features**: Historical production, weather patterns, equipment status
- **Validation**: Model accuracy assessment and performance metrics

### Model Implementation

```python
# Example usage of prediction model
from prediction_model import ProductionPredictor

predictor = ProductionPredictor()
predictor.train(historical_data)
next_day_prediction = predictor.predict(current_conditions)
```

## Development

### Running ETL Locally

```bash
cd etl
pip install -r requirements.txt
python coal_mining_etl.py
```

### Database Operations

```bash
# Connect to ClickHouse
docker exec -it clickhouse clickhouse-client --user=username --password=password

# View tables
SHOW TABLES FROM coal_mining;

# Query production metrics
SELECT * FROM coal_mining.daily_production_metrics LIMIT 10;
```

### Logs and Monitoring

```bash
# View ETL logs
docker logs coal-mining-etl

# Monitor service health
docker-compose ps
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CLICKHOUSE_HOST` | `clickhouse` | ClickHouse server hostname |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse server port |
| `CLICKHOUSE_USER` | `username` | Database username |
| `CLICKHOUSE_PASSWORD` | `password` | Database password |
| `CLICKHOUSE_DB` | `coal_mining` | Target database name |
| `METABASE_PORT` | `3000` | Metabase web interface port |
| `ETL_LOG_LEVEL` | `INFO` | ETL logging verbosity level |

### Custom Configuration

Modify `docker-compose.yml` to adjust:
- Resource allocation (CPU/memory limits)
- Port mappings
- Volume mounts
- Network configuration

## Performance Optimization

### Database Tuning

- **ClickHouse Configuration**: Optimized for analytical workloads
- **Indexing Strategy**: Proper indexing on date and mine_id columns
- **Partitioning**: Date-based partitioning for efficient queries
- **Compression**: LZ4 compression for storage optimization

### ETL Optimization

- **Batch Processing**: Configurable batch sizes for memory management
- **Parallel Processing**: Multi-threaded data processing where applicable
- **Caching**: Intelligent caching of API responses and intermediate results

## Security Considerations

- **Database Access**: Password-protected database connections
- **API Security**: Rate limiting and error handling for external API calls
- **Container Security**: Non-root user execution in containers
- **Data Privacy**: Anonymization of sensitive production data

## Troubleshooting

### Common Issues

1. **Service Startup Failures**
   - Check Docker daemon status
   - Verify port availability
   - Review container logs

2. **Database Connection Issues**
   - Confirm ClickHouse health status
   - Validate connection parameters
   - Check network connectivity

3. **ETL Processing Errors**
   - Review ETL logs in `etl/logs/`
   - Validate source data format
   - Check API endpoint availability

### Health Checks

```bash
# Check all services
docker-compose ps

# Verify ClickHouse
curl "http://localhost:8123/?user=username&password=password&query=SELECT 1"

# Test Metabase
curl http://localhost:3000/api/health
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License. See the LICENSE file for details.

## Support

For technical support or questions:
- Create an issue in the GitHub repository
- Review existing documentation in the `docs/` directory
- Check logs for error details and troubleshooting guidance

## Acknowledgments

- **Open-Meteo API**: Weather data provider
- **ClickHouse**: High-performance analytical database
- **Metabase**: Open-source business intelligence platform
- **Docker**: Containerization platform for deployment
