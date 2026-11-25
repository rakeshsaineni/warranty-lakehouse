select claim_id, amount from {{ ref('fct_claims') }} where amount < 0
