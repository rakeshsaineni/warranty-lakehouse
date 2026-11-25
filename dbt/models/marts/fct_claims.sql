-- One row per claim, with the dealer's region *as it was on the claim date* (point-in-time join on SCD2)
select
    c.claim_id,
    c.claim_date,
    c.component,
    c.status,
    c.amount,
    c.labor_hours,
    v.vin,
    v.model,
    v.model_year,
    date_diff('month', v.in_service_date, c.claim_date) as months_in_service,
    d.dealer_id,
    d.region as dealer_region_at_claim
from {{ ref('stg_claims') }} c
join {{ ref('stg_vehicles') }} v on v.vin = c.vin
join {{ ref('stg_dealers') }} d
  on d.dealer_id = c.dealer_id
 and cast(c.claim_date as timestamp) >= cast(d.valid_from as date)
 and (d.valid_to is null or cast(c.claim_date as timestamp) < cast(d.valid_to as date))
