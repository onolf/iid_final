select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from {{ source('br_ecommerce', 'sellers') }}
where seller_id is not null
