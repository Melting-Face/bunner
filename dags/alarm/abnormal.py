from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pypika import Query, Table
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from sklearn.impute import KNNImputer
from tabulate import tabulate

WINDOW_SIZE = 21
WEEK_SPAN = 52

def get_price_min_max_scale(series: pd.Series) -> pd.Series:
    _max = series.max()
    _min = series.min()
    scaled_series = (series - _min) / (_max - _min)
    return scaled_series

def get_price_min_max_adjusted_scale(series: pd.Series) -> pd.Series:
    _max = series.max() * 0.9
    _min = series.min() * 1.1
    scaled_series = (series - _min) / (_max - _min)
    return scaled_series

def calculate_band(series: pd.Series, sigma: float = 3.5):
    i, start, window_size = 0, 0, WINDOW_SIZE

    one_month = np.timedelta64(1, 'm')
    upper_band = []
    lower_band = []

    print(series)

    while i < len(series) - window_size + 1:
        if (series.index[i + window_size - 1] - series.index[start]) / one_month > 3:
            start += 1
        window = series[start : i + window_size]
        upper = sigma * window.std() + window.mean()
        lower = -sigma * window.std() + window.mean()

        upper_band.append(upper)
        lower_band.append(lower)
        i += 1

    upper_band = pd.Series(upper_band, index=series.index[window_size - 1 :])
    lower_band = pd.Series(lower_band, index=series.index[window_size - 1 :])
    if len(series) < window_size:
        empty_series = pd.Series([np.nan] * len(series), index=series.index)
    else:
        empty_series = pd.Series(
            [np.nan] * (window_size - 1), index=series.index[: window_size - 1]
        )

    upper_band = pd.concat([empty_series, upper_band])
    lower_band = pd.concat([empty_series, lower_band])
    return upper_band, lower_band

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
        df = df.withColumn("entry_id", F.col("entry_id").cast(T.IntegerType()))
        df = df.select("entry_id").distinct()
        df.show()
        entries = df.rdd.map(lambda x: x["entry_id"]).collect()
        spark.stop()
        return entries

    @task
    def make_query(entries):
        price = Table("price_price")
        date_to = date.today()
        date_from = date_to - timedelta(weeks=WEEK_SPAN)
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

        print(f"""
{tabulate(df, headers="keys")}
        """)
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

    @task
    def scale_price(df: pd.DataFrame):
        df['price_avg'] = df['price_avg'].astype(float)
        df['price_avg_imputed'] = df['price_avg_imputed'].astype(float)

        df['price_avg_scaled'] = get_price_min_max_scale(df['price_avg'])
        df['price_avg_imputed_scaled'] = get_price_min_max_scale(df['price_avg_imputed']) # noqa

        df['price_avg_adjusted_scaled'] = get_price_min_max_adjusted_scale(df['price_avg']) # noqa
        df['price_avg_imputed_adjusted_scaled'] = get_price_min_max_adjusted_scale(df['price_avg_imputed']) # noqa

        print(f"""
{tabulate(df, headers="keys")}
        """)

        return df

    @task
    def get_band(df: pd.DataFrame):
        df = df.set_index('date')
        df['upper_band'], df['lower_band'] = calculate_band(df['price_avg_imputed_adjusted_scaled']) # noqa
        print(f"""
{tabulate(df, headers="keys")}
        """)
        return df

    @task
    def save_csv(df: pd.DataFrame):
        df.to_csv("test.csv") 

    entries = get_abnormal_entries()
    sql = make_query(entries)
    df = get_df(sql)
    df = impute_price(df)
    df = scale_price(df)
    df = get_band(df)
    save_csv(df)

calculate()
