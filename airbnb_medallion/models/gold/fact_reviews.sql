{{ config(materialized='table') }}

with reviews as (
    select * from {{ ref('stg_reviews') }}
),

listings as (
    select * from {{ ref('stg_listings') }}
),

review_facts as (
    select
        r.review_id,
        r.listing_id,
        r.review_date,
        r.reviewer_id,
        r.reviewer_name,
        r.review_comments,
        l.neighbourhood,
        l.room_type,
        l.price_per_night,
        extract(year from r.review_date) as review_year,
        extract(month from r.review_date) as review_month,
        extract(dayofweek from r.review_date) as review_day_of_week
    from reviews r
    left join listings l on r.listing_id = l.listing_id
)

select * from review_facts
