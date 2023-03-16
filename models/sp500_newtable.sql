{{ config(materialized='table') }}

with newtable as (
    select
        *,
        100.*(t.Close - t.Open)/t.Open as gain, 

    from `mythic-byway-375404.stocks.sp500` t
)

select * from newtable
