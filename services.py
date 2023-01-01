import random
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from functools import reduce


def get_task_I(champs: DataFrame):
    win = champs.select("id", "name", "win")
    pick = champs.select("id", "name", "pick")
    ban = champs.select("id", "name", "ban")

    return (win, pick, ban)


def get_task_II(champs: DataFrame):
    syn_col_name = "synergy"
    syn = champs.select("id", "name",
                        ((col("win")/col("duo"))*100).alias(syn_col_name)).where(col(syn_col_name) < 100)

    return syn


def get_task_III(champs: DataFrame, items: DataFrame, names: DataFrame):

    champs_wins = champs.select("id", "win").toDF("championId", "wins")
    items_wins: DataFrame = items.join(champs_wins, on="championId", how="full").groupBy(
        "itemId").sum("wins", "pick").toDF("id", "wins", "pick")

    items_wins = items_wins.join(names, on=['id'])

    wins = items_wins\
        .select("id", "wins", "name")

    picks = items_wins\
        .select("id", "pick", "name")

    return (wins, picks)


def get_task_IV(champs: DataFrame, items: DataFrame, names: DataFrame):

    syn_col_name = "synergy"
    items = items.withColumn("id", col("itemId")).join(names, on=['id'])
    champs_wins = champs.select(champs.id.alias(
        "championId"), champs.name.alias("championName"), "win", "duo", "class1", "class2")
    combined_items_and_champs: DataFrame = items.join(
        champs_wins, on="championId", how="full")
    champ_analysis: DataFrame = combined_items_and_champs.groupBy(
        "id", "name", "championName").sum("win", "duo").toDF("id", "name", "championName", "duo", "win")\
        .select("id", "name",  "championName", ((col("win")/col("duo"))*100).alias(syn_col_name)).where(col(syn_col_name) < 100)
    class_analysis = combined_items_and_champs.select(
        "id", "name", "win", "duo", "class1", "class2")
    ca1 = combined_items_and_champs.select(
        "id", "name", "win", "duo", col("class1").alias("class"))
    ca2 = combined_items_and_champs.select(
        "id", "name", "win", "duo", col("class2").alias("class"))

    class_analysis: DataFrame = reduce(
        DataFrame.unionAll,
        [
            ca1,
            ca2
        ])
    class_analysis = class_analysis.groupBy("id", "name", "class").sum(
        "win", "duo").toDF("id",  "name", "class", "duo", "win")\
        .select("id", "name", "class",  ((col("win")/col("duo"))*100).alias(syn_col_name)).where(col(syn_col_name) < 100)\
        .dropna()

    return (champ_analysis, class_analysis)


def get_task_V(champs: DataFrame, champs_analysis: DataFrame, spark: SparkSession):
    syn_col_name = "synergy"
    champs = champs.dropna()
    chosen_ids = []
    chosen_classes = []
    items_list = []

    ids = champs.select("id").collect()

    while len(chosen_ids) < 4:
        i = random.choice(ids)
        champ = champs.where(col("id") == i.id)
        if champ.first().name == None:
            continue
        c1 = champ.first().class1
        c2 = champ.first().class2
        cs = []
        if c2 != None:
            cs = [c1, c2]
        else:
            cs = [c1]
        if cs not in chosen_classes and i not in chosen_ids:
            c = champs_analysis.where(
                col("championName") == champ.first().name).sort(col(syn_col_name).desc()).first()
            if c != None:
                chosen_classes.append(cs)
                chosen_ids.append(i.id)
                items_list.append(c)
    chosen_champs = champs.where(col("id").isin(chosen_ids))
    items: DataFrame = spark.createDataFrame(items_list)
    return (chosen_champs, items)
