select distinct
    to_number(to_char(sales_date, 'YYYYMMDD')) as date_id,
    sales_date as store_date,
    is_holiday,
    current_timestamp() as insert_date,
    current_timestamp() as update_date
from {{ ref('stg_department_sales') }}