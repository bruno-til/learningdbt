with amount as(
    select order_id, 
    sum (case when status = 'success' then amount end) as amount 
    from {{ref('stg_stripe_payments')}}
    group by 1
),

orders as(
    select *
    from {{ref('stg_jaffle_shop__orders')}}
)

select
a.order_id,
o.customer_id,
coalesce(a.amount, 0) as amount
from amount a
left join orders o 
on a.order_id = o.order_id