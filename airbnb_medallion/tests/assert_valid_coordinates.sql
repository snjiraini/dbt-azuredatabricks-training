select *
from {{ ref('dim_listings') }}
where latitude < -90 or latitude > 90
   or longitude < -180 or longitude > 180
