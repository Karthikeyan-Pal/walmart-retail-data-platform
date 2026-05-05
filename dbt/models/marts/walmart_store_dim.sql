with dept_store as (
    select distinct
        store_id,
        dept_id
    from {{ ref('stg_department_sales') }}
),

stores as (
    select
        store_id,
        store_type,
        store_size
    from {{ ref('stg_stores') }}
)

select
    ds.store_id,
    ds.dept_id,
    s.store_type,
    s.store_size,
    current_timestamp() as insert_date,
    current_timestamp() as update_date
from dept_store ds
left join stores s
    on ds.store_id = s.store_id