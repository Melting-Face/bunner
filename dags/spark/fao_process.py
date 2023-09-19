from datetime import datetime

from airflow.decorators import dag, task
from constants import VERBOSE_COUNTRY_MAP
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dag(start_date=datetime(2023, 9, 18), schedule=None)
def fao_process():
    @task
    def fao_aggregate():
        spark: SparkSession = SparkSession.builder.getOrCreate()

        # columns:
        # Element
        # Months,
        # Unit
        # Year(period)
        # Value(price)
        # Area(producer)
        # Item Code(code)
        def fao_price() -> DataFrame:
            print("fao_price")
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
            return df

        # columns:
        # Element
        # Months,
        # Unit
        # Year(period)
        # Value(weight)
        # Area(producer)
        # Item Code(code)
        # item
        def fao_data():
            print("fao_data")
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
            return df

        def aggregate_production_code(df: DataFrame):
            print("aggregate_production_code")
            df = df.select("code", F.col("Item").alias("name")).distinct()

            # result
            df.show()
            df.printSchema()
            print(f"total count: {df.count()}")
            return df

        def aggregate_production_data(
            data_df: DataFrame,
            price_df: DataFrame,
        ) -> DataFrame:
            print("aggregate_production_data")
            data_df = data_df.drop("item")
            df = data_df.join(price_df, on=["period", "producer", "code"], how="outer")

            # result
            df.show()
            df.printSchema()
            print(f"total count: {df.count()}")
            return df


        price_df = fao_price()
        data_df = fao_data()
        code_df = aggregate_production_code(data_df)
        data_df = aggregate_production_data(data_df, price_df)

        spark.stop()

    fao_aggregate()

fao_process()
