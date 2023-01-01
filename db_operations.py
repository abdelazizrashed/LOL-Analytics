from pyspark.sql import SparkSession, DataFrame
from constants import *
from clean_data import *


def save_file(file_name: str, count: int, spark: SparkSession, verbose: bool = False):
    spark.sql(
        f"insert into {CONFIG_TABLE} values (\"{file_name}\", {count})")
    if (verbose):
        df = get_data(CONFIG_TABLE, spark)
        df.show()
        df.printSchema()
        print(f"number of rows ===> {df.count()}")


def get_data_count(spark: SparkSession) -> int:
    df = get_data(CONFIG_TABLE, spark)
    return df.groupBy().sum().collect()[0][0]


def get_data(table_name: str, spark: SparkSession) -> DataFrame:
    df = spark.sql(f"select * from {table_name}")
    return df


def insert_df_to_table_and_overwrite(table_name: str, df: DataFrame, spark: SparkSession):

    df.createOrReplaceTempView("sampleView")

    spark.sql(
        f"INSERT OVERWRITE TABLE {table_name}   SELECT * FROM sampleView")


def add_items_data(df: DataFrame, spark: SparkSession):
    items = get_items(df)
    old_items = spark.sql(f'select * from {ITEMS_TABLE}')
    new_items = reduce(
        DataFrame.unionAll,
        [
            items,
            old_items,
        ])
    c_items: DataFrame = new_items.groupBy("championId", "itemId")\
        .sum("pick").toDF("championId", "itemId", "pick")
    insert_df_to_table_and_overwrite(ITEMS_TABLE, c_items, spark)


def add_champoins_data(df: DataFrame, spark: SparkSession):
    champs = get_champions(df, spark)
    info = champs.select("id", "class1", "class2")
    old = spark.sql(f'select * from {CHAMPIONS_TABLE}')
    new: DataFrame = reduce(
        DataFrame.unionAll,
        [
            champs,
            old,
        ]
    )

    c_champs: DataFrame = new.groupBy("id", "name").sum(
        "duo", "pick", "win", "ban")\
        .toDF("id", "name", "duo", "pick", "win", "ban")

    c_champs = c_champs.join(info, on="id", how="full")
    insert_df_to_table_and_overwrite(CHAMPIONS_TABLE, c_champs, spark)
