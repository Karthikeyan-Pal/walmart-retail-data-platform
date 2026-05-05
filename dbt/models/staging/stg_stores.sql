select
    try_to_number(store_id) as store_id,
    trim(store_type) as store_type,
    try_to_number(store_size) as store_size,
    source_file_name,
    load_ts
from {{ source('walmart_bronze','stores_raw') }}
qualify row_number() over (
    partition by try_to_number(store_id)
    order by load_ts desc, source_file_name desc
) = 1