select
    product_id,
    product_category_name                              as category_name_pt,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from {{ source('br_ecommerce', 'products') }}
where product_id is not null
