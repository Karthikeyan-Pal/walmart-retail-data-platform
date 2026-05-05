{{ config(
    materialized='incremental',
    unique_key='fact_row_id',
    incremental_strategy='merge'
) }}

with source_data as (

    select
        ds.store_id,
        ds.dept_id,
        dd.date_id,
        ds.weekly_sales,
        f.fuel_price,
        f.temperature,
        f.unemployment,
        f.cpi,
        f.markdown1,
        f.markdown2,
        f.markdown3,
        f.markdown4,
        f.markdown5,

        md5(
            concat(
                coalesce(cast(ds.store_id as varchar), ''), '|',
                coalesce(cast(ds.dept_id as varchar), ''), '|',
                coalesce(cast(dd.date_id as varchar), ''), '|',
                coalesce(cast(ds.weekly_sales as varchar), ''), '|',
                coalesce(cast(f.fuel_price as varchar), ''), '|',
                coalesce(cast(f.temperature as varchar), ''), '|',
                coalesce(cast(f.unemployment as varchar), ''), '|',
                coalesce(cast(f.cpi as varchar), ''), '|',
                coalesce(cast(f.markdown1 as varchar), ''), '|',
                coalesce(cast(f.markdown2 as varchar), ''), '|',
                coalesce(cast(f.markdown3 as varchar), ''), '|',
                coalesce(cast(f.markdown4 as varchar), ''), '|',
                coalesce(cast(f.markdown5 as varchar), '')
            )
        ) as record_hash

    from {{ ref('stg_department_sales') }} ds
    join {{ ref('walmart_date_dim') }} dd
        on ds.sales_date = dd.store_date
    left join {{ ref('stg_features') }} f
        on ds.store_id = f.store_id
       and ds.sales_date = f.sales_date
),

current_rows as (

    {% if is_incremental() %}
    select *
    from {{ this }}
    where is_current = true
    {% else %}
    select
        cast(null as varchar) as fact_row_id,
        cast(null as number) as store_id,
        cast(null as number) as dept_id,
        cast(null as number) as date_id,
        cast(null as number) as weekly_sales,
        cast(null as number) as fuel_price,
        cast(null as number) as temperature,
        cast(null as number) as unemployment,
        cast(null as number) as cpi,
        cast(null as number) as markdown1,
        cast(null as number) as markdown2,
        cast(null as number) as markdown3,
        cast(null as number) as markdown4,
        cast(null as number) as markdown5,
        cast(null as timestamp) as insert_ts,
        cast(null as timestamp) as update_ts,
        cast(null as timestamp) as vrsn_start_date,
        cast(null as timestamp) as vrsn_end_date,
        cast(null as boolean) as is_current,
        cast(null as varchar) as record_hash
    where 1 = 0
    {% endif %}
),

changed_or_new as (

    select
        s.*
    from source_data s
    left join current_rows c
        on s.store_id = c.store_id
       and s.dept_id = c.dept_id
       and s.date_id = c.date_id
    where c.record_hash is null
       or s.record_hash <> c.record_hash
),

expired_rows as (

    {% if is_incremental() %}
    select
        c.fact_row_id,
        c.store_id,
        c.dept_id,
        c.date_id,
        c.weekly_sales,
        c.fuel_price,
        c.temperature,
        c.unemployment,
        c.cpi,
        c.markdown1,
        c.markdown2,
        c.markdown3,
        c.markdown4,
        c.markdown5,
        c.insert_ts,
        current_timestamp() as update_ts,
        c.vrsn_start_date,
        current_timestamp() as vrsn_end_date,
        false as is_current,
        c.record_hash
    from current_rows c
    join changed_or_new s
        on s.store_id = c.store_id
       and s.dept_id = c.dept_id
       and s.date_id = c.date_id
    {% else %}
    select * from current_rows where 1 = 0
    {% endif %}
),

new_rows as (

    select
        md5(
            concat(
                cast(store_id as varchar), '|',
                cast(dept_id as varchar), '|',
                cast(date_id as varchar), '|',
                cast(current_timestamp() as varchar)
            )
        ) as fact_row_id,
        store_id,
        dept_id,
        date_id,
        weekly_sales,
        fuel_price,
        temperature,
        unemployment,
        cpi,
        markdown1,
        markdown2,
        markdown3,
        markdown4,
        markdown5,
        current_timestamp() as insert_ts,
        current_timestamp() as update_ts,
        current_timestamp() as vrsn_start_date,
        cast(null as timestamp) as vrsn_end_date,
        true as is_current,
        record_hash
    from changed_or_new
)

select * from new_rows

{% if is_incremental() %}
union all
select * from expired_rows
{% endif %}