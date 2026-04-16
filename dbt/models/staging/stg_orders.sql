with source as (
    select * from {{ source('br_ecommerce', 'orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    cast(order_purchase_timestamp as date)       as order_purchase_date,
    cast(order_approved_at as date)              as order_approved_date,
    cast(order_delivered_carrier_date as date)   as order_delivered_carrier_date,
    cast(order_delivered_customer_date as date)  as order_delivered_customer_date,
    cast(order_estimated_delivery_date as date)  as order_estimated_delivery_date,

    datediff(
        'day',
        cast(order_purchase_timestamp as date),
        cast(order_delivered_customer_date as date)
    ) as actual_delivery_days,

    datediff(
        'day',
        cast(order_estimated_delivery_date as date),
        cast(order_delivered_customer_date as date)
    ) as delivery_delay_days,

    case
        when order_delivered_customer_date <= order_estimated_delivery_date
        then true else false
    end as is_on_time

from source
where order_status = 'delivered'
  and order_delivered_customer_date is not null
