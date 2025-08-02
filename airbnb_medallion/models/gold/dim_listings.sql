{{ config(materialized='table') }}

with listings as (
    select * from {{ ref('stg_listings') }}
),

listing_metrics as (
    select
        listing_id,
        listing_name,
        host_id,
        host_name,
        neighbourhood_group,
        neighbourhood,
        latitude,
        longitude,
        room_type,
        price_per_night,
        minimum_nights,
        number_of_reviews,
        last_review_date,
        reviews_per_month,
        calculated_host_listings_count,
        availability_365,
        case
            when price_per_night < 50 then 'Budget'
            when price_per_night between 50 and 150 then 'Mid-range'
            when price_per_night between 150 and 300 then 'Premium'
            else 'Luxury'
        end as price_category,
        case
            when number_of_reviews = 0 then 'No Reviews'
            when number_of_reviews between 1 and 10 then 'Few Reviews'
            when number_of_reviews between 11 and 50 then 'Some Reviews'
            else 'Many Reviews'
        end as review_category
    from listings
)

select * from listing_metrics
