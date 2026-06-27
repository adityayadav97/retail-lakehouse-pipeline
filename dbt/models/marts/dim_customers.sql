-- Customer dimension enriched with lifetime value tiers, sourced from Gold.

with ltv as (

    select *
    from read_parquet('../lakehouse/gold/customer_ltv/*.parquet')

),

final as (

    select
        cast(customer_id as bigint)     as customer_id,
        name,
        email,
        region,
        cast(signup_date as date)       as signup_date,
        cast(lifetime_value as double)  as lifetime_value,
        cast(total_orders as integer)   as total_orders,
        case
            when lifetime_value >= 5000 then 'Platinum'
            when lifetime_value >= 2000 then 'Gold'
            when lifetime_value >= 500  then 'Silver'
            else 'Bronze'
        end as ltv_tier
    from ltv

)

select * from final
