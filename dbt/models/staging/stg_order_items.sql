select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    cast(shipping_limit_date as date) as shipping_limit_date,
    price,
    freight_value
from {{ source('br_ecommerce', 'order_items') }}
