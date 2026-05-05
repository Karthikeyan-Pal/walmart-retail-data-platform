with src as (
    select
        try_to_number(store_id) as store_id,
        try_to_number(dept_id) as dept_id,
        to_date(store_date) as sales_date,
        try_to_number(weekly_sales) as weekly_sales,
        case
            when upper(is_holiday) = 'TRUE' then true
            else false
        end as is_holiday,
        source_file_name,
        load_ts
    from {{ source('walmart_bronze','department_sales_raw') }}
),

deduped as (
    select *
    from src
    qualify row_number() over (
        partition by store_id, dept_id, sales_date
        order by load_ts desc, source_file_name desc
    ) = 1
),

final as (
    select
        store_id,
        dept_id,
        sales_date,
        case
            when store_id = 1
             and dept_id = 1
             and sales_date = '2010-02-05'
            then weekly_sales + 2000
            else weekly_sales
        end as weekly_sales,
        is_holiday,
        source_file_name,
        load_ts
    from deduped
)

select * from final