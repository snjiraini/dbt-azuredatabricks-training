{{ config(materialized='table') }}

with source_data as (
    select * from {{ ref('seed_full_moon_dates') }}
),

cleaned_data as (
    select
        cast(date as date) as full_moon_date
    from source_data
    where date is not null
)

select * from cleaned_data
