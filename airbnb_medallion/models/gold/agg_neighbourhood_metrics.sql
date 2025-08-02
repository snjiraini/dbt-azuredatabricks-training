{{ config(materialized='table') }}

with listings as (
    select * from {{ ref('dim_listings') }}
),

reviews as (
    select * from {{ ref('fact_reviews') }}
),

neighbourhood_stats as (
    select
        l.neighbourhood,
        l.neighbourhood_group,
        count(distinct l.listing_id) as total_listings,
        avg(l.price_per_night) as avg_price,
        min(l.price_per_night) as min_price,
        max(l.price_per_night) as max_price,
        avg(l.number_of_reviews) as avg_reviews_per_listing,
        sum(l.number_of_reviews) as total_reviews,
        count(distinct l.host_id) as unique_hosts,
        avg(l.availability_365) as avg_availability
    from listings l
    group by l.neighbourhood, l.neighbourhood_group
)

select * from neighbourhood_stats
