select *
from {{ ref('dim_listings') }}
where price_per_night <= 0
