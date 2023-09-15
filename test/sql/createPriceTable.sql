CREATE IF NOT EXISTS TABLE PRICES (
    SOURCE VARCHAR,
    DATE VARCHAR,
    UNIT VARCHAR,
    PAGEURL VARCHAR,
    PRICEAVG FLOAT,
    PRICEMAX FLOAT,
    PRICEMIN FLOAT,
    PRODUCT VARCHAR
) WITH (
    kafka_topic='prices',
    value_format='json',
    partitions=1
)
