from datetime import datetime

from airflow.decorators import dag, task
from constants import VERBOSE_COUNTRY_MAP
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


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

        # filter period
        df = df.withColumn("period", F.col("Year").cast(T.IntegerType()))
        df = df.where(F.col("period").isNotNull())

        # filter price
        df = df.withColumn("price", F.col("Value").cast(T.FloatType()))
        df = df.where(F.col("price").isNotNull())

        # convert area
        area_map_list = [(key, value) for key, value in VERBOSE_COUNTRY_MAP.items()]
        area_df = spark.createDataFrame(area_map_list, ["Area", "producer"])
        df = df.join(area_df, ["Area"])

        # normalize code
        df = df.withColumn(
            "code",
            F.lpad(F.col("Item Code").cast(T.StringType()), 4, "0")
        )

        # select
        df = df.select(["period", "producer", "code", "price"])

        # result
        df.show()
        df.printSchema()
        print(f"total count: {df.count()}")

        spark.stop()

    @task
    def fao_production():
        spark: SparkSession = SparkSession.builder.getOrCreate()  # noqa
        df = spark.read.csv(
            path='./Production_Crops_Livestock_E_All_Data_(Normalized).csv',
            header=True,
        )

        # filter Elements
        df = df.where(
            (F.col("Element") == "Production")
            & (F.col("Unit") == "tonnes")
        )

        # filter period
        df = df.withColumn("period", F.col("Year").cast(T.IntegerType()))
        df = df.where(F.col("period").isNotNull())

        # filter weight
        df = df.withColumn("weight", F.col("Value").cast(T.LongType()))
        df = df.where(F.col("weight").isNotNull())

        # convert area
        area_map_list = [(key, value) for key, value in VERBOSE_COUNTRY_MAP.items()]
        area_df = spark.createDataFrame(area_map_list, ["Area", "producer"])
        df = df.join(area_df, ["Area"])

        # normalize code
        df = df.withColumn(
            "code",
            F.lpad(F.col("Item Code").cast(T.StringType()), 4, "0")
        )

        # select
        df = df.select(["period", "producer", "code", "weight", "item"])

        # result
        df.show()
        df.printSchema()
        print(f"total count: {df.count()}")

        spark.stop()

    fao_production()
    fao_price()

fao_process()
