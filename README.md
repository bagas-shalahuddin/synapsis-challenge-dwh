# Coal Mining Data Pipeline

A containerized data pipeline to process coal mining data from production logs, equipment sensors, and weather APIs. Outputs are stored in ClickHouse and visualized using Metabase. Please check docs folder for the documents

## Stack

- PostgreSQL – stores production logs  
- CSV – sensor data (equipment status, fuel use)  
- Open-Meteo API – fetches daily weather  
- Python – runs batch ETL jobs  
- ClickHouse – stores processed metrics  
- Metabase – builds dashboards  
- Docker Compose – deploys full stack

## Quick Start

```bash
git clone <repo>
cd metabase-clickhouse
docker-compose up -d
````

To run ETL manually:

```bash
cd etl
python coal_mining_etl.py
```

Open dashboard at:
[http://localhost:3000](http://localhost:3000)

## Key Metrics

* Daily production volume
* Average coal grade
* Equipment utilization (%)
* Fuel per ton mined
* Weather impact on output

## Notes

* Weather API limited to 92 past days
* ETL uses caching to reduce redundant API calls
* Sensor CSV must be kept fresh manually or via cron
* System is batch-based, not real-time
* ClickHouse is optimized with partitions and compression

