{{ config(
    location_root='s3a://warehouse',
    file_format='delta',
    post_hook="optimize {{ this }}"
) }}

-- SELECT
--   TO_DATE(SRC:"Date yyyy-mm-dd"::VARCHAR) DATE,
--   SRC:"Supplier"::VARCHAR EXPORTER,
--   SRC:"Company declarant"::VARCHAR IMPORTER,
--   SRC:"Country of Origin"::VARCHAR ORIGIN_COUNTRY,
--   SRC:"Country of Sales"::VARCHAR EXPORT_COUNTRY,
--   'Peru' IMPORT_COUNTRY,
--   SRC:"Product Schedule B Code"::VARCHAR SCHEDULE_B_CODE,
--   SRC:"Harmonized Code/Product English"::VARCHAR HS_CODE,
--   SRC:"Product Description"::VARCHAR PRODUCT_DESCRIPTION,
--   SRC:"Commercial Brand"::VARCHAR BRAND,
--   SRC:"Product Description by Schedule B Code"::VARCHAR HS_CODE_DESCRIPTION,
--   TO_DOUBLE(REPLACE(SRC:"TOTAL Quantity 1"::VARCHAR, ',', '')) QUANTITY,
--   SRC:"Measure Unit 1 (Quantity-1)"::VARCHAR QUANTITY_UNIT,
--   TO_DOUBLE(REPLACE(SRC:"TOTAL Net Weight (Kg)"::VARCHAR, ',', '')) NET_WEIGHT_KG,
--   TO_DOUBLE(REPLACE(SRC:"TOTAL Gross Weight (Kg)"::VARCHAR, ',', '')) GROSS_WEIGHT_KG,
--   TO_DOUBLE(REPLACE(SRC:"TOTAL CIF Value (US$)"::VARCHAR, ',', '')) TOTAL_PRICE_USD,
--   TO_DOUBLE(REPLACE(SRC:"TOTAL FOB Value (US$)"::VARCHAR, ',', '')) TOTAL_FOB_USD,
--   TO_DOUBLE(REPLACE(SRC:"TOTAL CIF Value (US$)"::VARCHAR, ',', '')) TOTAL_CIF_USD,
--   SRC:"Type of Transport"::VARCHAR TRANSPORT,
--   SRC:"Transport Company"::VARCHAR TRANSPORT_COMPANY,
--   SRC:"Transaction Term"::VARCHAR INCOTERMS,
--   SRC:"Way of Payment"::VARCHAR WAY_OF_PAYMENT,
--   SRC:"Type of Packaging"::VARCHAR PACKAGING,
--   TO_TIMESTAMP_NTZ(SRC:"crawledAt") CRAWLED_AT
-- FROM {DATABASE_PROD}.LANDING.TRADE_SICEX
-- WHERE
--   SRC:"Source" = 'Sisduan Peru'
--   AND SRC:"Trade" = 'Imports'

select
    to_date("Date yyyy-mm-dd") date,
    supplier exporter,
    "Company declarant" importer,
    "Country of Origin" origin_country,
    "Country of Sales" export_country,
    'Peru' import_country
from {{ source('trade', 'sicex') }}
where
    source = 'Sisduan Peru'
    and trade = 'Imports'
