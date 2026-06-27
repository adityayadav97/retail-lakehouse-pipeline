-- Staging model: clean, typed view over the Gold daily_sales Delta export.
-- In production this would read from the Gold Delta tables registered in the
-- warehouse catalog. Here we read the exported parquet/delta as an external source.

with source as (

    select *
    from read_parquet('../lakehouse/gold/daily_sales/*.parquet')

),

renamed as (

    select
        cast(order_date as date)        as sales_date,
        cast(total_orders as integer)   as total_orders,
        cast(total_revenue as double)   as total_revenue,
        cast(avg_order_value as double) as avg_order_value,
        cast(unique_customers as integer) as unique_customers
    from source

)

select * from renamed
