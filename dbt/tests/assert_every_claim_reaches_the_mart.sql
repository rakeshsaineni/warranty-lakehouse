-- The point-in-time dealer join must neither drop nor duplicate claims
select (select count(*) from {{ ref('stg_claims') }}) as staged, (select count(*) from {{ ref('fct_claims') }}) as mart
where (select count(*) from {{ ref('stg_claims') }}) <> (select count(*) from {{ ref('fct_claims') }})
