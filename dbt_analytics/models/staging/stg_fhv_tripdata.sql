{{ config(materialized="view") }}

with tripdata as
(
    select *,
        row_number() over(partition by dispatching_base_num, pickup_datetime ) as rn
    from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null
)

select 
    --identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_number,
    cast(affiliated_base_number as string) as affiliated_base_number,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- shared trips
    cast(sr_flag  as integer) as sr_flag

from tripdata
-- where rn = 1
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var ('is_test_run', default=true) %}

    --limit 100 

{% endif%}

