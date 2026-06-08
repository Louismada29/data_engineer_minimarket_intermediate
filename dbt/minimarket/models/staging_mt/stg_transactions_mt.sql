-- Multi-tenant staging: transactions, completed only
select
    tenant_id,
    transaction_id,
    customer_id,
    store_id,
    transaction_date,
    toDate(transaction_date)    as transaction_day,
    total_amount,
    lower(trim(payment_method)) as payment_method,
    lower(trim(status))         as status
from {{ source('raw_mt', 'transactions') }} final
where lower(trim(status)) = 'completed'
