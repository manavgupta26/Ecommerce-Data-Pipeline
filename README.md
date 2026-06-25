# E-Commerce Data Pipeline

An end-to-end data engineering project that builds a complete ETL pipeline for e-commerce data using Apache Airflow, PostgreSQL, Docker, and Python.

The pipeline follows the Medallion Architecture (Bronze → Silver → Gold) to ingest raw data, clean and transform it, and create analytics tables that can be used directly for reporting and dashboards.

## Overview

This project simulates a real-world e-commerce data warehouse. Product data is collected from APIs, while orders, inventory, customers, and marketing campaign data are generated to mimic production workloads.

The pipeline automates the complete data flow from ingestion to analytics and produces datasets for tracking revenue, customer behavior, inventory, and campaign performance.

## Architecture

```
Data Sources
     │
     ▼
Bronze Layer
(Raw Data)
     │
     ▼
Silver Layer
(Cleaned & Validated)
     │
     ▼
Gold Layer
(Business Analytics)
     │
     ▼
Metabase Dashboard
```

### Bronze Layer

Stores raw data exactly as it is received from the source. Every record includes metadata such as ingestion time and source information.

Tables include:

* Products
* Customers
* Orders
* Inventory
* Campaigns

### Silver Layer

The Silver layer cleans and standardizes the raw data by:

* Removing duplicates
* Validating data types
* Handling missing values
* Enforcing relationships between tables
* Preparing data for analytics

### Gold Layer

The Gold layer contains business-ready tables for reporting and dashboards.

It includes:

* Daily revenue
* Product performance
* Customer RFM segmentation
* Inventory health
* Campaign ROI

## Tech Stack

* Apache Airflow
* PostgreSQL
* Python
* Docker & Docker Compose
* Pandas
* Faker
* Requests
* Metabase

## Features

* Automated ETL pipeline using Airflow DAGs
* Medallion Architecture (Bronze, Silver, Gold)
* Incremental data loading
* Data quality validation
* Historical tracking using SCD Type 2
* Customer RFM segmentation
* Inventory monitoring
* Campaign ROI analysis
* Business-ready analytics tables
* Interactive dashboards with Metabase

## Project Structure

```
.
├── dags/
├── sql/
├── scripts/
├── data/
├── logs/
├── plugins/
├── tests/
├── docker-compose.yml
└── README.md
```

## Getting Started

Clone the repository.

```bash
git clone https://github.com/yourusername/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

Start all services.

```bash
docker-compose up --build
```

Open Airflow.

```
http://localhost:8080
```

Default credentials:

```
Username: airflow
Password: airflow
```

Configure the warehouse connection in Airflow and trigger the Bronze pipeline. The remaining DAGs will execute according to their dependencies.

If you want to visualize the data, open Metabase.

```
http://localhost:3000
```

## Pipeline Workflow

1. Ingest raw data into Bronze tables.
2. Clean and validate data in the Silver layer.
3. Build analytics tables in the Gold layer.
4. Visualize business metrics using Metabase.

## Analytics

The pipeline generates several business metrics, including:

* Daily sales
* Revenue trends
* Best-selling products
* Customer segmentation using RFM
* Inventory status
* Marketing campaign performance

## Future Improvements

Some features that can be added later include:

* Streaming data ingestion with Kafka
* Data lake storage using S3 or MinIO
* Spark for large-scale processing
* CI/CD pipeline
* Data quality monitoring with Great Expectations
* Automated alerts for pipeline failures

## Author

**Manav Gupta**
