{{ config(materialized="view") }}

select
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationID as integer) as pickup_locationid,
    cast(DOlocationID as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as sr_flag,
    cast(Affiliated_base_number as string) as affiliated_base_number
from {{ source('staging', 'fhv_data_partitioned_clustered') }}
where dispatching_base_num is not null

-- dbt build -m <model.sql> --vars '{"is_test_run": false}'
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
