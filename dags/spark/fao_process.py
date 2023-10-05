from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from constants import VERBOSE_COUNTRY_MAP
from pypika import Query, Table

# from pypika.dialects import SnowflakeQuery as Query
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


@dag(start_date=datetime(2023, 9, 18), schedule=None)
def fao_process():
    @task
    def table_to_csv(table_name):
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        information = Table("information_schema.columns")
        table = Table(table_name)
        query = Query.from_(information).select("column_name").where(
            information.table_name == table_name
        ).get_sql(quote_char=None)
        print(f"query: {query}")

        records = hook.get_records(f"{query};")
        columns = [record[0] for record in records]

        query = Query.from_(table).select('*').get_sql(quote_char=None)
        records = hook.get_records(f"{query};")

        df = pd.DataFrame(columns=columns, data=records)
        df.to_csv(f"{table_name}.csv")

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

        def generate_world(data_df: DataFrame) -> DataFrame:
            print("generate_world")
            wl_data_df: DataFrame = data_df.groupBy("period", "code")
            wl_data_df = wl_data_df.agg(F.sum("weight").alias("weight"))

            wl_data_df = wl_data_df.withColumns({
                "producer": F.lit("WL"),
                "price": F.lit(None),
            })

            wl_data_df = wl_data_df.where(F.col("weight").isNotNull())

            fields = ["period", "producer", "code", "weight", "price"]
            data_df = data_df.select(fields)
            wl_data_df = wl_data_df.select(fields)
            data_df = data_df.union(wl_data_df)

            # result
            data_df.show()
            data_df.printSchema()
            print(f"total count: {data_df.count()}")
            return data_df

        def generate_total(data_df: DataFrame) -> DataFrame:
            print("generate_total")
            total_df = data_df.groupBy("period", "producer").agg(
                F.sum("weight").alias("weight"),
                F.when(
                    F.count(F.when(F.col("price").isNull(), True)) > 0,
                    None,
                ).otherwise(
                    F.sum("price")
                ).alias("price"),
                F.lit("TOTAL").alias("code"),
            )
            fields = ["period", "producer", "code", "weight", "price"]
            data_df = data_df.select(fields)
            total_df = total_df.select(fields)
            data_df = data_df.union(total_df)

            data_df.show()
            data_df.printSchema()
            print(f"total count: {data_df.count()}")
            return data_df

        def generate_trend(data_df: DataFrame) -> DataFrame:
            print("generate_trend")
            trend_df = data_df.groupBy("producer", "code").agg(
                F.collect_list(F.struct(["period", "weight", "price"]))
            )

            trend_df.show()
            trend_df.printSchema()
            print(f"total count: {trend_df.count()}")

        price_df = fao_price()
        data_df = fao_data()
        aggregate_production_code(data_df)
        data_df = aggregate_production_data(data_df, price_df)
        data_df = generate_world(data_df)
        data_df = generate_total(data_df)
        generate_trend(data_df)

        spark.stop()

    fao_aggregate()
    # table_to_csv.expand(table_name=['insight_producecode', 'fao_produce'])

fao_process()
