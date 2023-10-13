from datetime import datetime

from airflow.decorators import dag, task
from pyspark.sql import DataFrame, SparkSession


@dag(start_date=datetime(2023, 10, 13), schedule=None)
def csv_to_insert_query():
    @task
    def make_query():
        spark: SparkSession = SparkSession.builder.getOrCreate()
        df: DataFrame = spark.read.csv("UNSD-Methodology.csv")
        df.show()

    make_query()

csv_to_insert_query()
