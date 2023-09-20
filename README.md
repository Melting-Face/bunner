# DEV ENV

## Linter

### python

- ruff

### sql

- sqlfluff

### typescript

- eslint


## Formatter

### python

- ruff

- black

### sql

- sqlfluff

### typescript

- eslint

- prettier

# DEV COMMAND

## conda

### create virtual env
```shell
conda create -n airflow python=3.11 pyspark
```


## ksqldb

### cli
```shell
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```
