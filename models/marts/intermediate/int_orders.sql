{{ config(materialized='view') }}

WITH orders as (
    select * from {{ ref('stg_jaffle_shop__orders')}}
),

payments as (
    select * from {{ ref('stg_stripe__payments') }}
    where status != 'fail'
),

order_totals as (
   select order_id, status, sum(amount) amount
   from payments 
   group by 1,2
),
order_values_joined as (
    select orders.*, ot.amount
    from orders
    left join order_totals ot ON ot.order_id = orders.order_id and ot.status = orders.status
)

select * from order_values_joined