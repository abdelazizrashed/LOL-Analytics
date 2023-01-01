import json
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (explode, col, arrays_zip, lit)
from functools import reduce
from pyspark.sql.types import StructType, StructField, StringType, LongType


def get_champions(df: DataFrame, spark: SparkSession) -> DataFrame:

    stats = get_champ_team_stats(df)

    champoins = df.select(col("participants.championId"),
                          col("participants.championName"), col("participants.role"))\
        .toDF("championId", "championName", "role")

    champoins = champoins\
        .withColumn("temp", arrays_zip(col("championId"),
                                       col("championName"), col("role")))\
        .withColumn("temp", explode("temp"))\
        .select(col("temp.championId"), col("temp.championName"), col("temp.role"))\
        .withColumn("pick", lit(1))
    champoins = champoins.withColumn("role", champoins.role == "DUO")
    champoins = champoins.withColumn("role", champoins.role.cast("integer"))

    champoins: DataFrame = champoins.groupBy("championId", "championName").sum(
        "role", "pick").toDF("id", "name", "duo", "pick")

    champoins = champoins.join(stats, on="id", how="full").fillna(0)

    info = get_champ_info(spark)
    champoins = champoins.join(info, on="id", how="full")
    return champoins


def get_champ_team_stats(df: DataFrame) -> DataFrame:

    champoins_stats = df.select("teams").select(
        "teams.bans", "teams.win").toDF("bans", "win", )
    champoins_stats = champoins_stats\
        .withColumn("temp", arrays_zip(
            col("bans"), col("win")))\
        .withColumn("temp", explode("temp"))\
        .select(col("temp.bans"), col("temp.win"))
    champoins_stats = champoins_stats\
        .select(
            explode("bans"), 'win', )\
        .select("col.championId", "win")\
        .toDF("championId", "win")
    champoins_stats: DataFrame = champoins_stats\
        .withColumn("win", champoins_stats.win.cast("integer"))\
        .withColumn("ban", lit(1))\
        .groupBy(
            "championId").sum("win", 'ban').toDF("id", "win", "ban")
    champoins_stats = champoins_stats.where(
        champoins_stats.id > 0)
    return champoins_stats


def get_champ_info(spark: SparkSession) -> DataFrame:

    champions_file = "champions.json"
    data = {}
    with open(champions_file, "r") as f:
        data: dict = json.load(f)["data"]
    info = []
    for value in data.values():
        l = [int(value["key"])] + value["tags"]
        if len(l) < 3:
            l = l+[None]
        info.append(tuple(l))
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("class1", StringType(), True),
        StructField("class2", StringType(), True)
    ])

    return spark.createDataFrame(data=info, schema=schema)


def get_item_info(spark: SparkSession) -> DataFrame:
    items_file = "items.json"
    data = {}
    with open(items_file, "r") as f:
        data: dict = json.load(f)["data"]
    pairs = []
    for key, value in data.items():
        pairs.append((int(key), value["name"]))
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True)
    ])

    return spark.createDataFrame(data=pairs, schema=schema)


def get_items(df: DataFrame) -> DataFrame:
    items = df.select(explode("participants"))
    items = items.select(
        col("col.championId"),
        col("col.item0"),
        col("col.item1"),
        col("col.item2"),
        col("col.item3"),
        col("col.item4"),
        col("col.item5"),
        col("col.item6"),)
    i0 = items.select("championId", "item0").toDF("championId", "itemId")
    i1 = items.select("championId", "item1").toDF("championId", "itemId")
    i2 = items.select("championId", "item2").toDF("championId", "itemId")
    i3 = items.select("championId", "item3").toDF("championId", "itemId")
    i4 = items.select("championId", "item4").toDF("championId", "itemId")
    i5 = items.select("championId", "item5").toDF("championId", "itemId")
    i6 = items.select("championId", "item6").toDF("championId", "itemId")
    items = reduce(
        DataFrame.unionAll,
        [
            i0,
            i1,
            i2,
            i3,
            i4,
            i5,
            i6,
        ])
    items = items.withColumn("pick", lit(1))
    items: DataFrame = items.groupBy("championId", "itemId")\
        .sum("pick").toDF("championId", "itemId", "pick")
    return items
