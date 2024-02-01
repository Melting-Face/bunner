{{ config(
    location_root='s3a://warehouse',
    file_format='delta',
    post_hook="optimize {{ this }}"
) }}


select
    idx,
    item_id,
    `To Correct?` to_correct,
    `product name` product_name,
    `variety name` variety_name,
    `grade name` grade_name,
    `other attributes name` other_attributes_name,
    remark,
    origin_type,
    origin_country,
    origin_region,
    origin_continent,
    status,
    assignee,
    evaluator,
    comment,
    spreadsheet_key
from {{ source('price', 'migration') }};
