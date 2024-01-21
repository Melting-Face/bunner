# DOCKER IMAGES

- [trino](https://hub.docker.com/r/trinodb/trino)

- [spark](https://hub.docker.com/r/apache/spark-py)

- [hive](https://hub.docker.com/r/apache/hive)

- [postgres](https://hub.docker.com/_/postgres)

- [minio](https://hub.docker.com/r/minio/minio)

# DBT

## profiles.yml

```yaml
<profile_name>:
  outputs:
    dev:
      host: localhost
      method: thrift
      port: 10000
      schema: bronze
      threads: 1
      type: spark
  target: dev
```

## packages.yml

```yaml
packages:
  - package: dbt-labs/codegen
    version: 0.12.1
  - package: dbt-labs/dbt_project_evaluator
    version: 0.8.1
  - package: dbt-labs/dbt_utils
    version: 1.1.1
  - package: dbt-labs/dbt_external_tables
    version: 0.8.7
```
