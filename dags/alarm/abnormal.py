from datetime import date, datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pypika import Query, Table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sklearn.impute import KNNImputer
from tabulate import tabulate

PRICE_ABNORMAL_ALARM_WINDOW_SIZE = 21
ABNORMAL_WEEK_SPAN = 52

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
        date_to = date.today()
        date_from = date_to - timedelta(weeks=ABNORMAL_WEEK_SPAN)
        query = Query.from_(price).select(*columns).where(
            price.entry_id.isin([entries[0]])
            & price.date[date_from:date_to]
            & price.period == 'd'
        )
        print(query)
        return f"{query};"

    @task
    def get_df(sql):
        date_to = date.today()
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        records = hook.get_records(sql)
        df = pd.DataFrame(columns=columns, data=records)
        df['date'] = pd.to_datetime(df['date'])

        daily = {'date': pd.date_range(min(df['date']), date_to, freq='D')}
        daily_df: pd.DataFrame = pd.DataFrame(daily)
        df = daily_df.merge(df, left_on=['date'], right_on=['date'], how='left')

        print(tabulate(df, headers="keys"))
        return df

    @task
    def impute_price(df):
        df['date_category'] = df['date'].astype('category').cat.codes
        imputer = KNNImputer(n_neighbors=2)
        imputed_df = pd.DataFrame(
            imputer.fit_transform(df[['date_category', 'price_avg']]),
            columns=['date_category', 'price_avg_imputed'],
        )
        df: pd.DataFrame = df.merge(
            imputed_df,
            left_on=['date_category'],
            right_on=['date_category'],
            how='left',
        )
        df = df.drop('date_category', axis=1)
        print(f"""
{tabulate(df, headers="keys")}
        """)
        return df



    entries = get_abnormal_entries()
    sql = make_query(entries)
    df = get_df(sql)
    df = impute_price(df)

calculate()
