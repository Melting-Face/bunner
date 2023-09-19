CREATE TABLE IF NOT EXISTS NEWS (
    SOURCE VARCHAR,
    DATE VARCHAR,
    BODY VARCHAR,
    IMAGE VARCHAR,
    SUMMARY VARCHAR,
    TITLE VARCHAR,
    URL VARCHAR PRIMARY KEY
) WITH (
    kafka_topic='news',
    value_format='json',
    partitions=1
);
