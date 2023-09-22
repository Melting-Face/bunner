from datetime import date, datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pypika import Query, Table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from tabulate import tabulate

WEEK_SPAN = 52
COLUMNS=[
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

@dag(start_date=datetime(2023, 9, 19), schedule=None)
def save():
    @task
    def get_whitelist_entries():
        spark: SparkSession = SparkSession.builder.getOrCreate()
        df: DataFrame = spark.read.csv(header=True, path="./alarms/*.csv")
        df = df.withColumn("entry_id", F.col("entry_id").cast(T.IntegerType()))
        df = df.select("entry_id").distinct()
        df.show()
        entries = df.rdd.map(lambda x: x["entry_id"]).collect()
        print(entries)
        spark.stop()
        return entries

    @task
    def save_parquet(entry):
        price = Table("price_price")
        date_to = date.today()
        date_from = date_to - timedelta(weeks=WEEK_SPAN)
        query = Query.from_(price).select(*COLUMNS).where(
            price.entry_id.isin([entry])
            & price.date[date_from:date_to]
            & price.period == 'd'
        )
        print(query)

        date_to = date.today()
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        records = hook.get_records(f"{query};")
        df = pd.DataFrame(columns=COLUMNS, data=records)
        df['date'] = pd.to_datetime(df['date'])
        daily = {'date': pd.date_range(min(df['date']), date_to, freq='D')}
        daily_df: pd.DataFrame = pd.DataFrame(daily)
        df = daily_df.merge(
            df,
            left_on=['date'],
            right_on=['date'],
            how='left',
        )

        print(f"""
{tabulate(df, headers="keys")}
        """)

        df.to_parquet(
            f"whitelist_price/{entry}.parquet",
            compression=None
        )

    entries = get_whitelist_entries()
    save_parquet.expand(entry=entries)

save()
