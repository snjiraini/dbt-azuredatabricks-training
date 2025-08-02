{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('bronze', 'seed_full_moon_dates') }}
),

cleaned_data as (
    select
        cast(full_moon_date as date) as full_moon_date
    from source_data
    where full_moon_date is not null
)

select * from cleaned_data
