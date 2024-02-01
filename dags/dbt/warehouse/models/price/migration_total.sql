{{ config(
    location_root='s3a://warehouse',
    file_format='delta',
    post_hook="optimize {{ this }}"
) }}


select *
from {{ source('price', 'migration') }};
