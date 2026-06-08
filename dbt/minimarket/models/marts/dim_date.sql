-- Dimension: date. Generated natively in ClickHouse (no dbt_utils needed)
-- by exploding range() over the number of days between min/max txn date.
-- NOTE: weekday/month names are built via array lookup instead of
-- formatDateTime('%A'/'%b'), which is not supported on all ClickHouse versions.
with bounds as (
    select
        min(transaction_day) as min_day,
        dateDiff('day', min(transaction_day), max(transaction_day)) + 1 as n_days
    from {{ ref('stg_transactions') }}
),

spine as (
    select
        min_day + toIntervalDay(num) as date_day
    from bounds
    array join range(toUInt32(n_days)) as num
)

select
    {{ generate_surrogate_key(['date_day']) }} as date_key,
    date_day,
    toYear(date_day)        as year,
    toQuarter(date_day)     as quarter,
    toMonth(date_day)       as month,
    -- YYYY-MM built without format specifiers
    concat(toString(toYear(date_day)), '-', leftPad(toString(toMonth(date_day)), 2, '0')) as year_month,
    -- "Jan 2026" via 1-indexed array lookup (ClickHouse arrays are 1-based)
    concat(
        ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][toMonth(date_day)],
        ' ',
        toString(toYear(date_day))
    ) as month_label,
    toDayOfMonth(date_day)  as day_of_month,
    toDayOfWeek(date_day)   as day_of_week,   -- 1=Mon .. 7=Sun
    ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][toDayOfWeek(date_day)] as day_name,
    if(toDayOfWeek(date_day) >= 6, 1, 0) as is_weekend
from spine
order by date_day
