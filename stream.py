import numpy as np
import pandas as pd
import streamlit as st
from pyspark.sql import DataFrame, SparkSession
from sklearn.impute import KNNImputer

WINDOW_SIZE = 21
WEEK_SPAN = 52

@st.cache_data
def get_df():
    spark = SparkSession.builder.getOrCreate()
    df: DataFrame = spark.read.parquet("whitelist_price/*.parquet")
    df = df.toPandas()
    spark.stop()
    return df

@st.cache_data
def create_df(entry_id):
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
        return df

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

        while i < len(series) - window_size + 1:
            if (series.index[i + window_size - 1] - series.index[start]) / one_month > 3: # noqa
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

    def scale_price(df: pd.DataFrame):
        df['price_avg'] = df['price_avg'].astype(float)
        df['price_avg_imputed'] = df['price_avg_imputed'].astype(float)

        df['price_avg_scaled'] = get_price_min_max_scale(df['price_avg'])
        df['price_avg_imputed_scaled'] = get_price_min_max_scale(df['price_avg_imputed']) # noqa

        df['price_avg_adjusted_scaled'] = get_price_min_max_adjusted_scale(df['price_avg']) # noqa
        df['price_avg_imputed_adjusted_scaled'] = get_price_min_max_adjusted_scale(df['price_avg_imputed']) # noqa

        return df

    def get_band(df: pd.DataFrame):
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        df['upper_band'], df['lower_band'] = calculate_band(df['price_avg_imputed_adjusted_scaled']) # noqa
        return df

    df = pd.read_parquet(f"whitelist_price/{entry_id}.parquet")
    df = impute_price(df)
    df = scale_price(df)
    df = get_band(df)
    df = df.reset_index()
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return df

def callback():
    entry_id = st.session_state['entry_id']
    create_df(entry_id)

df = get_df()
options = set(df['entry_id'].unique())

option = st.selectbox(
    key='entry_id',
    label="Select entry_id?",
    options=options,
    on_change=callback
)
