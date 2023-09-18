from datetime import datetime

from airflow.decorators import dag, task
from constants import VERBOSE_COUNTRY_MAP
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dag(start_date=datetime(2023, 9, 18), schedule=None)
def fao_process():
    @task
    def fao_price():
        spark: SparkSession = SparkSession.builder.getOrCreate()  # noqa
        df = spark.read.csv(
            path='./Prices_E_All_Data_(Normalized).csv',
            header=True,
        )
        df = df.where(
            (F.col("Element") == "Producer Price (USD/tonne)")
            & (F.col("Months") == "Annual value")
            & (F.col("Unit") == "USD")
        )
        area_map_list = [(key, value) for key, value in VERBOSE_COUNTRY_MAP.items()]
        area_df = spark.createDataFrame(area_map_list, ["Area", "producer"])
        df = df.join(area_df, df["Area"] == area_df["Area"], "inner")
        df.show()
        spark.stop()

    @task
    def fao_production():
        spark: SparkSession = SparkSession.builder.getOrCreate()  # noqa
        df = spark.read.csv(
            path='./Production_Crops_Livestock_E_All_Data_(Normalized).csv',
            header=True,
        )
        df.show()
        spark.stop()

    fao_production()
    fao_price()

fao_process()
