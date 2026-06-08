-- Dimension: date (multi-tenant range). Built natively in ClickHouse.
with bounds as (
    select
        min(transaction_day) as min_day,
        dateDiff('day', min(transaction_day), max(transaction_day)) + 1 as n_days
    from {{ ref('stg_transactions_mt') }}
),
spine as (
    select min_day + toIntervalDay(num) as date_day
    from bounds
    array join range(toUInt32(n_days)) as num
)
select
    {{ generate_surrogate_key(['date_day']) }} as date_key,
    date_day,
    toYear(date_day)    as year,
    toQuarter(date_day) as quarter,
    toMonth(date_day)   as month,
    concat(toString(toYear(date_day)), '-', leftPad(toString(toMonth(date_day)), 2, '0')) as year_month,
    concat(['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][toMonth(date_day)], ' ', toString(toYear(date_day))) as month_label,
    toDayOfWeek(date_day) as day_of_week,
    ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][toDayOfWeek(date_day)] as day_name,
    if(toDayOfWeek(date_day) >= 6, 1, 0) as is_weekend
from spine
order by date_day
