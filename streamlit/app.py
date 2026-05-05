import os
from typing import Optional

import pandas as pd
import plotly.express as px
import snowflake.connector
import streamlit as st

st.set_page_config(page_title="Walmart Retail Analytics", layout="wide")


# -----------------------------
# Helpers
# -----------------------------
def _get_secret(path: str, default: Optional[str] = None) -> Optional[str]:
    try:
        keys = path.split(".")
        value = st.secrets
        for k in keys:
            value = value[k]
        return value
    except Exception:
        return os.getenv(path.replace(".", "_").upper(), default)


def build_connection_params() -> dict:
    return {
        "account": _get_secret("snowflake.account", "bp78023.us-east-2.aws"),
        "user": _get_secret("snowflake.user", "Karthileo"),
        "password": _get_secret("snowflake.password","Karthik#19970612"),
        "warehouse": _get_secret("snowflake.warehouse", "WALMART_WH"),
        "database": _get_secret("snowflake.database", "WALMART_DB"),
        "schema": _get_secret("snowflake.schema", "GOLD"),
        "role": _get_secret("snowflake.role", "ACCOUNTADMIN"),
    }


def render_connection_sidebar(defaults: dict) -> dict:
    st.sidebar.header("Snowflake Connection")
    with st.sidebar.expander("Connection Settings", expanded=True):
        params = {
            "account": st.text_input("Account", value=defaults.get("account", ""), help="Example: bp78023.us-east-2.aws"),
            "warehouse": st.text_input("Warehouse", value=defaults.get("warehouse", "WALMART_WH")),
            "database": st.text_input("Database", value=defaults.get("database", "WALMART_DB")),
            "schema": st.text_input("Schema", value=defaults.get("schema", "GOLD")),
            "role": st.text_input("Role", value=defaults.get("role", "ACCOUNTADMIN")),
        }
    return params


def format_currency_short(value: float) -> str:
    if pd.isna(value):
        return "$0"
    abs_value = abs(float(value))
    if abs_value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"${value/1_000:.2f}K"
    return f"${value:,.2f}"


def format_number_short(value: float) -> str:
    if pd.isna(value):
        return "0"
    abs_value = abs(float(value))
    if abs_value >= 1_000_000_000:
        return f"{value/1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{value/1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{value/1_000:.2f}K"
    return f"{value:,.0f}"


def apply_currency_axis(fig, axis: str = "y"):
    if axis == "y":
        fig.update_yaxes(tickprefix="$", tickformat="~s", separatethousands=True)
    else:
        fig.update_xaxes(tickprefix="$", tickformat="~s", separatethousands=True)
    return fig


@st.cache_resource(show_spinner=False)
def get_connection(account: str, user: str, password: str, warehouse: str, database: str, schema: str, role: str):
    return snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=role,
    )


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str, conn_params: tuple) -> pd.DataFrame:
    conn = get_connection(*conn_params)
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


def main():
    st.title("Walmart Retail Analytics Dashboard")
    st.caption("Snowflake + dbt + Streamlit presentation layer")

    with st.expander("Project Architecture", expanded=False):
        st.markdown(
            """
            - Raw CSV files land in Amazon S3
            - Snowflake `COPY INTO` loads Bronze tables
            - dbt builds Silver staging models
            - dbt builds Gold dimensions and the versioned fact table
            - Streamlit reads current Gold data for reporting
            """
        )

    defaults = build_connection_params()
    params = render_connection_sidebar(defaults)

    if not defaults.get("user") or not defaults.get("password"):
        st.error("Snowflake username/password are not configured. Add them in Streamlit secrets or environment variables.")
        st.stop()

    conn_params = (
        params["account"],
        defaults["user"],
        defaults["password"],
        params["warehouse"],
        params["database"],
        params["schema"],
        params["role"],
    )

    try:
        store_df = run_query(
            "select distinct store_id from WALMART_DB.GOLD.WALMART_FACT_TABLE where is_current = true order by 1",
            conn_params,
        )
        dept_df = run_query(
            "select distinct dept_id from WALMART_DB.GOLD.WALMART_FACT_TABLE where is_current = true order by 1",
            conn_params,
        )
        store_size_df = run_query(
            """
            select distinct store_size
            from WALMART_DB.GOLD.WALMART_STORE_DIM
            where store_size is not null
            order by 1
            """,
            conn_params,
        )
    except Exception as e:
        st.error(f"Snowflake connection/query failed: {e}")
        st.stop()

    st.sidebar.header("Business Filters")
    store_options = ["All"] + store_df["STORE_ID"].astype(str).tolist()
    dept_options = ["All"] + dept_df["DEPT_ID"].astype(str).tolist()
    holiday_options = ["All", "True", "False"]
    size_options = ["All"] + [str(x) for x in store_size_df["STORE_SIZE"].tolist()]

    selected_store = st.sidebar.selectbox("Store", store_options)
    selected_dept = st.sidebar.selectbox("Department", dept_options)
    selected_holiday = st.sidebar.selectbox("Is Holiday", holiday_options)
    selected_store_size = st.sidebar.selectbox("Store Size", size_options)

    where_clauses = ["f.is_current = true"]
    if selected_store != "All":
        where_clauses.append(f"f.store_id = {selected_store}")
    if selected_dept != "All":
        where_clauses.append(f"f.dept_id = {selected_dept}")
    if selected_holiday != "All":
        where_clauses.append(f"d.is_holiday = {'true' if selected_holiday == 'True' else 'false'}")
    if selected_store_size != "All":
        where_clauses.append(f"s.store_size = {selected_store_size}")
    where_sql = " AND ".join(where_clauses)

    base_query = f"""
        select
            f.store_id,
            f.dept_id,
            f.date_id,
            d.store_date,
            d.is_holiday,
            f.weekly_sales,
            f.fuel_price,
            f.temperature,
            f.unemployment,
            f.cpi,
            f.markdown1,
            f.markdown2,
            f.markdown3,
            f.markdown4,
            f.markdown5,
            s.store_type,
            s.store_size
        from WALMART_DB.GOLD.WALMART_FACT_TABLE f
        join WALMART_DB.GOLD.WALMART_DATE_DIM d
            on f.date_id = d.date_id
        left join WALMART_DB.GOLD.WALMART_STORE_DIM s
            on f.store_id = s.store_id
           and f.dept_id = s.dept_id
        where {where_sql}
        order by d.store_date
    """

    try:
        df = run_query(base_query, conn_params)
    except Exception as e:
        st.error(f"Failed to load reporting dataset: {e}")
        st.stop()

    if df.empty:
        st.warning("No data returned for the selected filters.")
        st.stop()

    df.columns = [c.upper() for c in df.columns]

    numeric_cols = [
        "STORE_ID", "DEPT_ID", "DATE_ID", "WEEKLY_SALES", "FUEL_PRICE", "TEMPERATURE",
        "UNEMPLOYMENT", "CPI", "MARKDOWN1", "MARKDOWN2", "MARKDOWN3", "MARKDOWN4", "MARKDOWN5", "STORE_SIZE"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["STORE_DATE"] = pd.to_datetime(df["STORE_DATE"])
    df["YEAR"] = df["STORE_DATE"].dt.year
    df["MONTH_NUM"] = df["STORE_DATE"].dt.month
    df["MONTH_NAME"] = df["STORE_DATE"].dt.strftime("%b")
    df["YEAR_MONTH"] = df["STORE_DATE"].dt.to_period("M").astype(str)
    month_order = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    df["MONTH_NAME"] = pd.Categorical(df["MONTH_NAME"], categories=month_order, ordered=True)
    df["IS_HOLIDAY_LABEL"] = df["IS_HOLIDAY"].map({True: "Holiday", False: "Non-Holiday"}).fillna("Unknown")

    # Executive KPIs
    st.subheader("Executive Overview")
    total_sales = df["WEEKLY_SALES"].sum()
    avg_sales = df["WEEKLY_SALES"].mean()
    num_stores = df["STORE_ID"].nunique()
    num_departments = df["DEPT_ID"].nunique()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Sales", format_currency_short(total_sales))
    col2.metric("Average Weekly Sales", format_currency_short(avg_sales))
    col3.metric("Stores", format_number_short(num_stores))
    col4.metric("Departments", format_number_short(num_departments))

    # Sales trend with holiday legend
    st.subheader("Sales Trend Over Time")
    trend_df = (
        df.groupby(["STORE_DATE", "IS_HOLIDAY_LABEL"], as_index=False)["WEEKLY_SALES"]
        .sum()
        .sort_values("STORE_DATE")
    )
    fig_trend = px.line(
        trend_df,
        x="STORE_DATE",
        y="WEEKLY_SALES",
        color="IS_HOLIDAY_LABEL",
        markers=True,
        title="Weekly Sales Trend by Holiday Flag",
    )
    apply_currency_axis(fig_trend)
    st.plotly_chart(fig_trend, use_container_width=True)

    # Store/department/store type performance
    row1_col1, row1_col2 = st.columns(2)
    with row1_col1:
        st.subheader("Sales by Store")
        store_perf_df = (
            df.groupby("STORE_ID", as_index=False)["WEEKLY_SALES"]
            .sum()
            .sort_values("WEEKLY_SALES", ascending=False)
        )
        fig_store = px.bar(store_perf_df, x="STORE_ID", y="WEEKLY_SALES", title="Total Sales by Store")
        apply_currency_axis(fig_store)
        st.plotly_chart(fig_store, use_container_width=True)

    with row1_col2:
        st.subheader("Sales by Department")
        dept_perf_df = (
            df.groupby("DEPT_ID", as_index=False)["WEEKLY_SALES"]
            .sum()
            .sort_values("WEEKLY_SALES", ascending=False)
        )
        fig_dept = px.bar(dept_perf_df, x="DEPT_ID", y="WEEKLY_SALES", title="Total Sales by Department")
        apply_currency_axis(fig_dept)
        st.plotly_chart(fig_dept, use_container_width=True)

    row2_col1, row2_col2 = st.columns(2)
    with row2_col1:
        st.subheader("Holiday vs Non-Holiday Sales")
        holiday_df = df.groupby("IS_HOLIDAY_LABEL", as_index=False)["WEEKLY_SALES"].sum()
        fig_holiday = px.bar(holiday_df, x="IS_HOLIDAY_LABEL", y="WEEKLY_SALES", title="Holiday vs Non-Holiday Sales")
        apply_currency_axis(fig_holiday)
        st.plotly_chart(fig_holiday, use_container_width=True)

    with row2_col2:
        st.subheader("Total Sales by Store Type")
        store_type_pie = df.groupby("STORE_TYPE", as_index=False)["WEEKLY_SALES"].sum()
        fig_store_type_pie = px.pie(store_type_pie, values="WEEKLY_SALES", names="STORE_TYPE", title="Total Sales by Store Type")
        st.plotly_chart(fig_store_type_pie, use_container_width=True)

    # Weekly sales by year/month/date separately
    st.subheader("Weekly Sales by Year, Month, and Date")
    y1, y2, y3 = st.columns(3)

    with y1:
        sales_year_df = df.groupby("YEAR", as_index=False)["WEEKLY_SALES"].sum().sort_values("YEAR")
        fig_year = px.bar(sales_year_df, x="YEAR", y="WEEKLY_SALES", title="Weekly Sales by Year")
        apply_currency_axis(fig_year)
        st.plotly_chart(fig_year, use_container_width=True)

    with y2:
        sales_month_df = (
            df.groupby(["MONTH_NUM", "MONTH_NAME"], as_index=False)["WEEKLY_SALES"]
            .sum()
            .sort_values("MONTH_NUM")
        )
        fig_month = px.line(sales_month_df, x="MONTH_NAME", y="WEEKLY_SALES", markers=True, title="Weekly Sales by Month")
        apply_currency_axis(fig_month)
        st.plotly_chart(fig_month, use_container_width=True)

    with y3:
        sales_date_df = df.groupby("STORE_DATE", as_index=False)["WEEKLY_SALES"].sum().sort_values("STORE_DATE")
        fig_date = px.line(sales_date_df, x="STORE_DATE", y="WEEKLY_SALES", title="Weekly Sales by Date")
        apply_currency_axis(fig_date)
        st.plotly_chart(fig_date, use_container_width=True)

    # Store type monthly sales
    st.subheader("Weekly Sales by Store Type and Month")
    store_type_month_df = (
        df.groupby(["YEAR_MONTH", "STORE_TYPE"], as_index=False)["WEEKLY_SALES"]
        .sum()
        .sort_values("YEAR_MONTH")
    )
    fig_store_type_month = px.line(
        store_type_month_df,
        x="YEAR_MONTH",
        y="WEEKLY_SALES",
        color="STORE_TYPE",
        markers=True,
        title="Weekly Sales by Store Type and Month",
    )
    apply_currency_axis(fig_store_type_month)
    st.plotly_chart(fig_store_type_month, use_container_width=True)

    # External factor analysis
    st.subheader("External Factor Analysis")
    c1, c2 = st.columns(2)
    with c1:
        fig_fuel_sales = px.scatter(df, x="FUEL_PRICE", y="WEEKLY_SALES", color="STORE_TYPE", title="Sales vs Fuel Price")
        apply_currency_axis(fig_fuel_sales)
        fig_fuel_sales.update_xaxes(tickprefix="$", separatethousands=True)
        st.plotly_chart(fig_fuel_sales, use_container_width=True)

        temp_year_df = df.copy()
        temp_year_df["YEAR"] = temp_year_df["YEAR"].astype(str)
        fig_temp = px.scatter(temp_year_df, x="TEMPERATURE", y="WEEKLY_SALES", color="YEAR", title="Weekly Sales by Temperature and Year")
        apply_currency_axis(fig_temp)
        st.plotly_chart(fig_temp, use_container_width=True)

    with c2:
        fig_cpi = px.scatter(df, x="CPI", y="WEEKLY_SALES", color="STORE_TYPE", title="Sales vs CPI")
        apply_currency_axis(fig_cpi)
        st.plotly_chart(fig_cpi, use_container_width=True)

        fig_unemp = px.scatter(df, x="UNEMPLOYMENT", y="WEEKLY_SALES", color="STORE_TYPE", title="Sales vs Unemployment")
        apply_currency_axis(fig_unemp)
        st.plotly_chart(fig_unemp, use_container_width=True)

    # Fuel price by year for each store type
    st.subheader("Fuel Price by Year and Store Type")
    fuel_year_df = (
        df.groupby(["YEAR", "STORE_TYPE"], as_index=False)["FUEL_PRICE"]
        .mean()
        .sort_values("YEAR")
    )
    fig_fuel_year = px.line(
        fuel_year_df,
        x="YEAR",
        y="FUEL_PRICE",
        color="STORE_TYPE",
        markers=True,
        title="Average Fuel Price by Year for Each Store Type",
    )
    fig_fuel_year.update_yaxes(tickprefix="$", separatethousands=True)
    st.plotly_chart(fig_fuel_year, use_container_width=True)

    # Store size impact
    st.subheader("Store Size Impact on Weekly Sales")
    store_size_sales_df = (
        df.groupby(["STORE_SIZE", "STORE_TYPE"], as_index=False)["WEEKLY_SALES"]
        .sum()
        .sort_values("STORE_SIZE")
    )
    fig_store_size = px.scatter(
        store_size_sales_df,
        x="STORE_SIZE",
        y="WEEKLY_SALES",
        color="STORE_TYPE",
        size="WEEKLY_SALES",
        title="Store Size vs Weekly Sales",
    )
    apply_currency_axis(fig_store_size)
    st.plotly_chart(fig_store_size, use_container_width=True)

    # SCD validation summary
    st.subheader("SCD Validation Summary")
    scd_metrics_sql = (
        "select "
        "(select count(*) from WALMART_DB.GOLD.WALMART_STORE_DIM) as store_dim_rows, "
        "(select count(*) from WALMART_DB.GOLD.WALMART_FACT_TABLE where is_current = true) as current_fact_rows, "
        "(select count(*) from WALMART_DB.GOLD.WALMART_FACT_TABLE where is_current = false) as historical_fact_rows"
    )
    scd_metrics = run_query(scd_metrics_sql, conn_params)
    m1, m2, m3 = st.columns(3)
    m1.metric("Store Dim Rows (SCD1)", format_number_short(int(scd_metrics.iloc[0]["STORE_DIM_ROWS"])))
    m2.metric("Current Fact Rows (SCD2)", format_number_short(int(scd_metrics.iloc[0]["CURRENT_FACT_ROWS"])))
    m3.metric("Historical Fact Rows (SCD2)", format_number_short(int(scd_metrics.iloc[0]["HISTORICAL_FACT_ROWS"])))

    st.subheader("Sample Fact History")
    sample_history_query = """
        select
            store_id,
            dept_id,
            date_id,
            weekly_sales,
            vrsn_start_date,
            vrsn_end_date,
            is_current
        from WALMART_DB.GOLD.WALMART_FACT_TABLE
        where store_id = 1 and dept_id = 1
        order by date_id, vrsn_start_date
        limit 50
    """
    sample_history_df = run_query(sample_history_query, conn_params)
    sample_history_df.columns = [c.upper() for c in sample_history_df.columns]
    if "WEEKLY_SALES" in sample_history_df.columns:
        sample_history_df["WEEKLY_SALES"] = pd.to_numeric(sample_history_df["WEEKLY_SALES"], errors="coerce").map(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    st.dataframe(sample_history_df, use_container_width=True)


if __name__ == "__main__":
    main()
