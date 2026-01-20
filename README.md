# 🛒 E-Commerce Data Pipeline

A production-grade, end-to-end data engineering project built with **Apache Airflow**, **PostgreSQL**, and **Docker**. This pipeline implements a **medallion architecture** (Bronze → Silver → Gold) to process e-commerce data and generate business intelligence insights.

![Pipeline Architecture](https://img.shields.io/badge/Architecture-Medallion-blue) ![Airflow](https://img.shields.io/badge/Airflow-2.8.1-orange) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue) ![Docker](https://img.shields.io/badge/Docker-Compose-blue) ![Python](https://img.shields.io/badge/Python-3.11-green)

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Features](#features)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [DAG Workflows](#dag-workflows)
- [Data Model](#data-model)
- [Analytics & Visualizations](#analytics--visualizations)
- [Production Features](#production-features)
- [Sample Queries](#sample-queries)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)
- [License](#license)

---

## 🎯 Overview

This project demonstrates a **real-world data engineering pipeline** that:
- Ingests data from REST APIs and generates realistic e-commerce transactions
- Implements data quality checks and transformations
- Creates business-ready analytics with **RFM segmentation**, **inventory health monitoring**, and **campaign ROI analysis**
- Tracks historical changes using **SCD Type 2** (Slowly Changing Dimensions)
- Provides self-service BI dashboards with **Metabase**

**Use Case**: E-commerce analytics platform for tracking sales, customers, inventory, and marketing campaigns.

---

## 🏗️ Architecture

### Medallion Architecture
```
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐   │
│  │ Fake Store   │  │    Order     │  │  Inventory   │   │
│  │     API      │  │  Generator   │  │   Generator  │   │
│  └──────────────┘  └──────────────┘  └──────────────┘   │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│              🥉 BRONZE LAYER (Raw Data)                 │
│  • bronze_products      • bronze_orders                 │
│  • bronze_customers     • bronze_inventory              │
│  • bronze_campaigns                                     │
│  ✓ Full audit trail     ✓ Source metadata               │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓ Data Quality Checks
                         │
┌─────────────────────────────────────────────────────────┐
│           🥈 SILVER LAYER (Cleaned & Validated)         │
│  • silver_products      • silver_orders                 │
│  • silver_customers     • silver_inventory              │
│  • silver_campaigns                                     │
│  ✓ Deduplicated        ✓ Type validated                 │
│  ✓ Enriched fields     ✓ Referential integrity          │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓ Aggregations & Analytics
                         │
┌─────────────────────────────────────────────────────────┐
│         🥇 GOLD LAYER (Business Analytics)              │
│  • gold_daily_revenue                                   │
│  • gold_product_performance                             │
│  • gold_customer_segments (RFM)                         │
│  • gold_inventory_health                                │
│  • gold_campaign_roi                                    │
│  ✓ Business KPIs       ✓ Ready for BI tools             │
└────────────────────────┬────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────┐
│              📊 VISUALIZATION LAYER                     │
│                    (Metabase)                           │
│  • Revenue Dashboards   • Customer Insights             │
│  • Product Analytics    • Inventory Alerts              │
└─────────────────────────────────────────────────────────┘
```

### Infrastructure Architecture
```
┌──────────────────────────────────────────────────────┐
│              DOCKER COMPOSE STACK                    │
├──────────────────────────────────────────────────────┤
│                                                      │
│  ┌────────────┐  ┌────────────┐  ┌─────────────┐     │
│  │  Airflow   │  │  Airflow   │  │  Warehouse  │     │
│  │ Webserver  │  │ Scheduler  │  │  PostgreSQL │     │
│  │ :8080      │  │            │  │  :5433      │     │
│  └────────────┘  └────────────┘  └─────────────┘     │
│                                                      │
│  ┌────────────┐  ┌────────────┐  ┌─────────────┐     │
│  │  Airflow   │  │  Metadata  │  │  Metabase   │     │
│  │   Worker   │  │ PostgreSQL │  │  :3000      │     │
│  │            │  │  :5432     │  │             │     │
│  └────────────┘  └────────────┘  └─────────────┘     │
│                                                      │
└──────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow | 2.8.1 |
| **Data Warehouse** | PostgreSQL | 15 |
| **Containerization** | Docker Compose | 3.8 |
| **Language** | Python | 3.11 |
| **Visualization** | Metabase | Latest |
| **Object Storage** | MinIO | Latest |
| **API Source** | Fake Store API | - |

### Python Libraries
- `apache-airflow-providers-postgres` - Database connectivity
- `pandas` - Data manipulation
- `faker` - Synthetic data generation
- `requests` - API calls
- `psycopg2-binary` - PostgreSQL adapter

---

## ✨ Features

### 🔄 Data Pipeline
- ✅ **Automated daily ingestion** from REST APIs
- ✅ **Incremental loading** with upsert logic
- ✅ **Data quality validation** (email format, price ranges, referential integrity)
- ✅ **Error handling** with retries and alerting
- ✅ **Full audit trail** (source, timestamp, pipeline_run_id)

### 📊 Analytics
- ✅ **RFM Customer Segmentation** (Recency, Frequency, Monetary)
- ✅ **Product Performance Metrics** (revenue, profit margin, rankings)
- ✅ **Inventory Health Monitoring** (low stock, overstock, dead stock alerts)
- ✅ **Campaign ROI Analysis** (ROAS, cost per order, conversion rates)
- ✅ **Daily Revenue Trends** (YoY growth, weekend patterns)

### 🗂️ Advanced Features
- ✅ **SCD Type 2** - Historical tracking of price changes and customer tier progression
- ✅ **XCom** - Inter-task communication for data sharing
- ✅ **External Task Sensors** - DAG dependency management
- ✅ **Dynamic Task Generation** - Scalable pipeline design
- ✅ **Metabase Integration** - Self-service BI dashboards

---

## 🚀 Setup Instructions

### Prerequisites
- Docker Desktop (4.0+)
- Docker Compose (3.8+)
- 8GB RAM minimum
- 10GB free disk space

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/ecommerce-data-pipeline.git
cd ecommerce-data-pipeline
```

2. **Create required directories**
```bash
mkdir -p dags logs plugins data/incoming data/archive sql scripts tests
```

3. **Build and start services**
```bash
docker-compose build
docker-compose up -d
```

4. **Wait for services to initialize** (2-3 minutes)
```bash
# Check service health
docker-compose ps
```

5. **Access Airflow UI**
- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

6. **Configure Airflow connection**
```bash
# Add warehouse database connection
docker exec -it  airflow connections add 'warehouse_db' \
    --conn-type 'postgres' \
    --conn-host 'warehouse-db' \
    --conn-schema 'data_warehouse' \
    --conn-login 'warehouse' \
    --conn-password 'warehouse' \
    --conn-port 5432
```

Or via Airflow UI:
- Go to Admin → Connections → Add
- Connection Id: `warehouse_db`
- Connection Type: `Postgres`
- Host: `warehouse-db`
- Schema: `data_warehouse`
- Login: `warehouse`
- Password: `warehouse`
- Port: `5432`

7. **Trigger the pipeline**
```bash
# In Airflow UI, unpause and trigger:
# 1. bronze_ingestion
# 2. silver_transformation (auto-triggers)
# 3. gold_analytics (auto-triggers)
# 4. scd_maintenance
```

8. **Access Metabase** (optional)
- URL: http://localhost:3000
- Setup account and connect to `warehouse-db:5432/data_warehouse`

---
