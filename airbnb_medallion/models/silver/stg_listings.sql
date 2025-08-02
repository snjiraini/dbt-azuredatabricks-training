{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('bronze', 'listings') }}
),

cleaned_data as (
    select
        try_cast(id as bigint) as listing_id,
        name as listing_name,
        try_cast(host_id as bigint) as host_id,
        host_name,
        neighbourhood_group,
        neighbourhood,
        latitude,
        longitude,
        room_type,
        cast(try_cast(regexp_replace(price, '[$,]', '') as double) as decimal(10,2)) as price_per_night,
        minimum_nights,
        number_of_reviews,
        case
            when last_review = '' then null
            else cast(last_review as date)
        end as last_review_date,
        reviews_per_month,
        calculated_host_listings_count,
        availability_365,
        ingested_at
    from source_data
    where id is not null
)

select * from cleaned_data
