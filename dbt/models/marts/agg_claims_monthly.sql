-- Claims per 1,000 vehicles in service, by model and component, with a 3-month rolling average
with months as (
    select distinct date_trunc('month', claim_date) as month from {{ ref('fct_claims') }}
),
in_service as (
    select m.month, v.model, count(*) as vehicles
    from months m
    join {{ ref('stg_vehicles') }} v on v.in_service_date < m.month + interval 1 month
    group by 1, 2
),
claims as (
    select date_trunc('month', claim_date) as month, model, component, count(*) as claims, sum(amount) as cost
    from {{ ref('fct_claims') }}
    group by 1, 2, 3
)
select
    c.month,
    c.model,
    c.component,
    c.claims,
    round(c.cost, 2) as cost,
    s.vehicles,
    round(1000.0 * c.claims / s.vehicles, 2) as claims_per_1k,
    round(avg(1000.0 * c.claims / s.vehicles) over (
        partition by c.model, c.component order by c.month rows between 2 preceding and current row), 2)
        as claims_per_1k_rolling_3m
from claims c
join in_service s on s.month = c.month and s.model = c.model
