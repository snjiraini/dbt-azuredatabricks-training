{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('bronze', 'reviews') }}
),

cleaned_data as (
    select
        try_cast(listing_id as bigint) as listing_id,
        try_cast(id as bigint) as review_id,
        try_cast(date as date) as review_date,
        try_cast(reviewer_id as bigint) as reviewer_id,
        reviewer_name,
        comments as review_comments,
        ingested_at
    from source_data
    where listing_id is not null
    and id is not null
    and date is not null
)

select * from cleaned_data
