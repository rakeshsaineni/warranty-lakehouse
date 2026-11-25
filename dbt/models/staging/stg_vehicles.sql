select vin, model, model_year, in_service_date
from {{ source('silver', 'vehicles') }}
