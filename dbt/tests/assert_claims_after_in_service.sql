select claim_id from {{ ref('fct_claims') }} where months_in_service < 0
