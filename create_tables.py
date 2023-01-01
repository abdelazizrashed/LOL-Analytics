from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, col, arrays_zip
from constants import *


def create_db(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")


def create_config(spark: SparkSession):
    spark.sql(
        f"CREATE TABLE {CONFIG_TABLE} (file_name string, count int)")


def create_champions(df: DataFrame, spark: SparkSession):
    df.createOrReplaceTempView("sampleView")
    spark.sql(
        f"CREATE TABLE {CHAMPIONS_TABLE} (id long, name String,duo long, pick long, win long, ban long, class1 string, class2 string)")

    spark.sql(
        f"INSERT INTO TABLE {CHAMPIONS_TABLE}   SELECT * FROM sampleView")


def create_items_names(df: DataFrame, spark: SparkSession):
    df.createOrReplaceTempView("sampleView")
    spark.sql(
        f"CREATE TABLE {ITEMS_NAMES_TABLE} (id long , name String)")

    spark.sql(
        f"INSERT INTO TABLE {ITEMS_NAMES_TABLE}   SELECT * FROM sampleView")


def create_items(df: DataFrame, spark: SparkSession):
    df.createOrReplaceTempView("sampleView")
    spark.sql(
        f"CREATE TABLE {ITEMS_TABLE} (championId long , itemId long , pick long)")

    spark.sql(
        f"INSERT INTO TABLE {ITEMS_TABLE}   SELECT * FROM sampleView")
