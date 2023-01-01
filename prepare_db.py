import json
from pyspark.sql import SparkSession, DataFrame
import findspark
from functools import reduce
from pyspark.sql.functions import explode, col, arrays_zip, lit

from clean_data import *
from create_tables import *
from db_operations import *
from utilities import *

verbose = input("Verbose...?\n0 for false\n1 for true\nInput: ")
if verbose == "1":
    verbose = True
elif verbose == "0":
    verbose = False
else:
    print("wrong value.... \nTerminate now")
    exit()

findspark.init()


file_name = "sample.json"

spark = SparkSession \
    .builder \
    .appName("lol") \
    .enableHiveSupport()\
    .getOrCreate()


create_db(spark)

print(f"Using file {file_name}....")

df = spark.read.json(file_name)

create_config(spark)

save_file(file_name, df.count(), spark, verbose=verbose)

print("Champions....")

champs = get_champions(df, spark)
if verbose:
    print("Champions data extracted from file")
    print_df_info(champs)

print("Creating table and inserting data")
create_champions(champs, spark)
if verbose:
    temp = get_data(CHAMPIONS_TABLE, spark)
    print("Champoins data in database")
    print_df_info(temp)

print("Items info....")

items_info = get_item_info(spark)
if verbose:
    print("Items info data extracted from items.json file")
    print_df_info(items_info)

print('Creating table.....')
create_items_names(items_info, spark)
if verbose:
    temp = get_data(ITEMS_NAMES_TABLE, spark)
    print("Items names in db")
    print_df_info(temp)

print("Items ......")
items = get_items(df)
if verbose:
    print("Items data extracted from file")
    print_df_info(items)
print("Creating table")
create_items(items, spark)
if verbose:
    temp = get_data(ITEMS_TABLE, spark)
    print("Items data in db")
    print_df_info(temp)

print("Next step is adding more data")
