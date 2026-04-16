{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

items_agg as (
    select
        order_id,
        sum(price)         as total_price,
        sum(freight_value) as total_freight,
        count(*)           as total_items
    from {{ ref('stg_order_items') }}
    group by order_id
),

customers as (
    select * from {{ ref('stg_customers') }}
),

weather as (
    select * from {{ ref('dim_weather') }}
)

select
    o.order_id,
    o.customer_id,
    c.customer_state,
    o.order_purchase_date,
    o.order_status,
    o.actual_delivery_days,
    o.delivery_delay_days,
    o.is_on_time,
    i.total_price,
    i.total_freight,
    i.total_items,
    w.weather_id,
    w.precipitation_mm,
    w.is_rainy_day,
    w.rain_category,
    w.temp_max_c

from orders o
left join customers      c on o.customer_id     = c.customer_id
left join items_agg      i on o.order_id        = i.order_id
left join weather        w on o.order_purchase_date = w.reference_date
                           and c.customer_state     = w.state_code
{% if is_incremental() %}
    where o.order_purchase_date > (select max(order_purchase_date) from {{ this }})
{% endif %}
