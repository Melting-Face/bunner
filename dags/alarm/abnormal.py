import pandas as pd

from datetime import datetime, date, timedelta

from pypika import Table, Query

from tabulate import tabulate

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


PRICE_ABNORMAL_ALARM_WINDOW_SIZE = 21
PRICE_ABNORMAL_ALARM_WINDOW_WEEK_SPAN = 52

@dag(start_date=datetime(2023, 9, 19), schedule=None)
def calculate():
    columns=[
        'entry_id',
        'id',
        'is_active',
        'currency',
        'date',
        'period',
        'price_min',
        'price_max',
        'price_avg',
    ]

    @task
    def get_abnormal_entries():
        spark: SparkSession = SparkSession.builder.getOrCreate()
        df: DataFrame = spark.read.csv(header=True, path="./alarms/*.csv")
        df = df.withColumn("is_abnormal", F.col("is_abnormal").cast(T.IntegerType()))
        df = df.withColumn("entry_id", F.col("entry_id").cast(T.IntegerType()))
        df = df.where(F.col("is_abnormal") == 1)
        df = df.select("entry_id")
        df.show()
        entries = df.rdd.map(lambda x: x["entry_id"]).collect()
        spark.stop()
        return entries

    @task
    def make_query(entries):
        price = Table("price_price")
        query = Query.from_(price).select(*columns).where(
            price.entry_id.isin(entries)
        )
        print(query)
        return f"{query};"

    @task
    def get_df(sql):
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        records = hook.get_records(sql)
        df = pd.DataFrame(columns=columns, data=records)
        print(tabulate(df, headers="keys"))
        return df

    entries = get_abnormal_entries()
    sql = make_query(entries)
    get_df(sql)

calculate()
