# Walmart Streamlit Presentation Layer

## Files
- `app.py` - Streamlit dashboard connected to Snowflake Gold tables
- `requirements.txt` - Python dependencies
- `.streamlit/config.toml` - basic Streamlit theme config

## Snowflake objects expected
- `WALMART_DB.GOLD.WALMART_FACT_TABLE`
- `WALMART_DB.GOLD.WALMART_DATE_DIM`
- `WALMART_DB.GOLD.WALMART_STORE_DIM`

## How to run
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Run the app:
   ```bash
   streamlit run app.py
   ```
3. Enter Snowflake credentials in the Streamlit sidebar.

## Recommended Snowflake connection values
- Warehouse: `WALMART_WH`
- Database: `WALMART_DB`
- Schema: `GOLD`
- Role: `ACCOUNTADMIN` or your project role

## Dashboard sections
- Executive Overview
- Sales Trend Over Time
- Sales by Store
- Sales by Department
- Holiday Analysis
- External Factor Analysis
- Markdown Impact
- SCD Validation Summary

## Presentation tip
Use `is_current = true` rows for business reporting. The SCD history section is only for demonstrating Type 2 behavior.
