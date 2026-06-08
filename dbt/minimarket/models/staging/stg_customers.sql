-- Staging: customers. Clean & standardise from raw landing.
with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    customer_id,
    trim(name)                         as customer_name,
    phone,
    lower(trim(email))                 as email,
    lower(trim(gender))                as gender,
    initCap(trim(city))                as city,
    created_at                         as created_at
from source
