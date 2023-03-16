{{ config(materialized='view') }}

with newtable as (
    select
        *,
        100.*(t.open - t.close)/t.open as gain, 

    from `mythic-byway-375404.stocks.nyse` t
)

select * from newtable
