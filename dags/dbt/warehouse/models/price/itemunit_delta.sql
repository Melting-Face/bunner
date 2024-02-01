{{ config(
    location_root='s3a://warehouse',
    file_format='delta',
    partition_by='country_id',
    post_hook="optimize {{ this }}"
) }}

with itemunit as (
    select *
    from
        {{ source('price', 'itemunit') }}
),

item as (
    select *
    from
        {{ source('price', 'item') }}
)

select
    item.country_id country_id,
    item.product_raw product_raw,
    item.source_id source_id,
    itemunit.id id,
    itemunit.unit unit,
    itemunit.unit_raw unit_raw,
    itemunit.unit_rate unit_rate,
    itemunit.unit_label unit_label,
    itemunit.item_id item_id,
    itemunit.evaluation_memo evaluation_memo,
    itemunit.evaluator_staff_id evaluator_staff_id,
    itemunit.submitter_staff_id submitter_staff_id,
    itemunit.status status
from itemunit
inner join item
    on itemunit.item_id = item.id;
