-- SCD Type 2: one row per version of each dealer
select dealer_id, name, region, valid_from, valid_to, is_current
from {{ source('silver', 'dealers_history') }}
