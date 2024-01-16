-- Use the `ref` function to select from other models

{{ config(
  materialized='table',
  location_root='s3a://warehouse/dbt',
  file_format='delta',
) }}

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
