select claim_id, vin, dealer_id, component, status, amount, claim_date, labor_hours
from {{ source('silver', 'claims') }}
