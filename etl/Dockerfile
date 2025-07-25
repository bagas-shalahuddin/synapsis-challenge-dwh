FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL scripts and data files
COPY coal_mining_etl.py .
COPY prediction_model.py .
COPY run_etl.sh .
COPY init.sql .
COPY equipment_sensors.csv .

# Make startup script executable
RUN chmod +x run_etl.sh

# Create directory for logs
RUN mkdir -p /app/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["./run_etl.sh"]
