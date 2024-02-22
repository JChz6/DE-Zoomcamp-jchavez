{{ config(materialized="view") }}

with fhv as (select * from {{ source("staging", "fhv_tripdata1") }})

select
    {{dbt_utils.generate_surrogate_key(["dispatching_base_num", "pickup_datetime"]) }}
    as tripid,
    dispatching_base_num,

    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,

    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("numeric")) }} as sr_flag,

    affiliated_base_number

from fhv

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
