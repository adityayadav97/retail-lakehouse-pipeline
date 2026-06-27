-- Fact mart: daily sales KPIs with a 7-day moving average of revenue.

with daily as (

    select * from {{ ref('stg_orders') }}

),

final as (

    select
        sales_date,
        total_orders,
        total_revenue,
        avg_order_value,
        unique_customers,
        round(
            avg(total_revenue) over (
                order by sales_date
                rows between 6 preceding and current row
            ), 2
        ) as revenue_7d_moving_avg
    from daily

)

select * from final
