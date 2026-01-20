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

┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Fake Store   │  │    Order     │  │  Inventory   │  │
│  │     API      │  │  Generator   │  │   Generator  │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  │
└────────────────────────┬────────────────────────────────┘
│
↓
┌─────────────────────────────────────────────────────────┐
│              🥉 BRONZE LAYER (Raw Data)                 │
│  • bronze_products      • bronze_orders                 │
│  • bronze_customers     • bronze_inventory              │
│  • bronze_campaigns                                     │
│  ✓ Full audit trail     ✓ Source metadata              │
└────────────────────────┬────────────────────────────────┘
│
↓ Data Quality Checks
│
┌─────────────────────────────────────────────────────────┐
│           🥈 SILVER LAYER (Cleaned & Validated)         │
│  • silver_products      • silver_orders                 │
│  • silver_customers     • silver_inventory              │
│  • silver_campaigns                                     │
│  ✓ Deduplicated        ✓ Type validated                │
│  ✓ Enriched fields     ✓ Referential integrity         │
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
│  ✓ Business KPIs       ✓ Ready for BI tools            │
└────────────────────────┬────────────────────────────────┘
│
↓
┌─────────────────────────────────────────────────────────┐
│              📊 VISUALIZATION LAYER                     │
│                    (Metabase)                           │
│  • Revenue Dashboards   • Customer Insights             │
│  • Product Analytics    • Inventory Alerts              │
└─────────────────────────────────────────────────────────┘
