select
    try_to_number(store_id) as store_id,
    to_date(store_date) as sales_date,
    temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    cpi,
    unemployment,
    case
        when upper(is_holiday) = 'TRUE' then true
        else false
    end as is_holiday
from {{ source('walmart_bronze','fact_raw') }}