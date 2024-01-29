{{ config(
    location_root='s3a://warehouse',
    file_format='delta',
    post_hook="optimize {{ this }}"
) }}

{%
  set payment_methods = [
    'exporter',
    'exporter_address',
    'importer',
    'importer_address',
    'origin_country',
    'export_country',
    'import_country',
    'schedule_b_code',
    'hs_code',
    'product_description',
    'brand',
    'hs_code_description',
    'quantity',
    'quantity_unit',
    'net_weight_kg',
    'gross_weight_kg',
    'total_price_usd',
    'total_fob_usd',
    'total_cif_usd',
    'transport',
    'port_of_loading',
    'port_of_discharging',
    'transport_company',
    'incoterm',
    'way_of_payment',
    'contact',
    'phone_number',
    'email',
    'packaging'
  ]
%}

with static_country as (
    select
        raw_country_name,
        mapped_country_code
    from {{ source('common', 'static_country_map') }}
    where
        is_mapped = true
),

merged_table as (
    select
      {% for column in columns %}
          coalesce({{column}}, NULL),
      {% endfor %}
      date
    from {{ source('trade', 'trade_sicex_argentina_import_dynamic') }}
)

select
  {% for column in columns %}
      merged_table.{{column}} {{column}},
  {% endfor %}
  merged_table.date date,
  sco.raw_country_name origin_country_code
from merged_table mt
-- left join static_country sco
-- on mt.origin_country = sco.raw_country_name;
