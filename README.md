# Walmart Retail Data Platform (Snowflake + dbt + Streamlit)

## Overview

This project builds an end-to-end retail analytics data platform using Snowflake, dbt, AWS S3, and Streamlit.

It simulates a real-world data engineering pipeline by ingesting raw data, transforming it into a dimensional model, implementing Slowly Changing Dimensions (SCD1 & SCD2), and exposing insights through an interactive dashboard.

---

## Architecture

AWS S3 → Snowflake Bronze → dbt Silver → dbt Gold → Streamlit Dashboard


---

## Tech Stack

- Snowflake (Data Warehouse)
- dbt (Transformation)
- AWS S3 (Storage)
- Streamlit (Visualization)
- Python, SQL

---

## Data Pipeline

### Bronze Layer
- Raw CSV data loaded from S3 using Snowflake `COPY INTO`
- Stores raw, unprocessed data

### Silver Layer
- Data cleaning and transformation using dbt
- Type casting, joins, and deduplication

### Gold Layer
- Dimensional modeling (Star Schema)
- Fact and Dimension tables
- Analytics-ready data

---

## Data Modeling

### Fact Table
Grain:
- One row per store, department, and week

Metrics:
- Weekly Sales
- Fuel Price
- Temperature
- CPI
- Unemployment

### Dimensions
- Date Dimension
- Store Dimension

---

## Slowly Changing Dimensions

- **SCD Type 1 (Store Dimension)**  
  Overwrites existing records when attributes change

- **SCD Type 2 (Fact Table)**  
  Tracks historical changes using:
  - version start date
  - version end date
  - current flag

---

## Dashboard Features

- Executive KPIs (Total Sales, Average Sales, Store Count)
- Sales Trend Over Time
- Sales by Store
- Sales by Department
- Holiday vs Non-Holiday Analysis
- External Factor Analysis (Temperature, Fuel Price, CPI, Unemployment)
- Store Type Analysis
- SCD Validation Summary

---

## How to Run

```bash
pip install -r streamlit/requirements.txt
python -m streamlit run streamlit/app.py