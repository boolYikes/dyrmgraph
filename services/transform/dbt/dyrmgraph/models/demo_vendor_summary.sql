{{ config(materialized='table') }}

select
    vendor_id,
    count(*) as trip_count,
    round(sum(fare_amount), 2) as total_fare
from {{ source('nyc', 'taxis') }}
group by vendor_id