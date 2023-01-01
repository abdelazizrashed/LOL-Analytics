from pyspark.sql import SparkSession
import findspark
from db_operations import *
from utilities import *
import os


findspark.init()


verbose = input("Verbose...?\n0 for false\n1 for true\nInput: ")
if verbose == "1":
    verbose = True
elif verbose == "0":
    verbose = False
else:
    print("wrong value.... \nTerminate now")
    exit()

file_name = input("Enter file path: ")

if not os.path.isfile(file_name):
    print("The file entered doesn't exist exiting now")
    exit()

spark = SparkSession \
    .builder \
    .appName("lol") \
    .enableHiveSupport()\
    .getOrCreate()

num = get_data_count(spark)
print(f"current number of matches ====> {num}")

print(f"Using file {file_name}....")

df = spark.read.json(file_name)

data_count = df.count()
print(data_count)

save_file(file_name, data_count, spark, verbose=verbose)

print("adding champions.....")


add_champoins_data(df, spark)


if verbose:
    temp = get_data(CHAMPIONS_TABLE, spark)
    print("Champoins data in database")
    print_df_info(temp)


add_items_data(df, spark)

if verbose:
    temp = get_data(ITEMS_TABLE, spark)
    print("Items data in db")
    print_df_info(temp)

num = get_data_count(spark)
print(f"current number of matches ====> {num}")
