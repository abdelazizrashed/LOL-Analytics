

import random
from pyspark.sql import SparkSession, DataFrame
import findspark

from pyspark.sql.functions import explode, col, arrays_zip, lit, when


from clean_data import *

from db_operations import *

from services import *


findspark.init()

spark = SparkSession \
    .builder \
    .appName("lol") \
    .enableHiveSupport()\
    .getOrCreate()
num_matches = get_data_count(spark)

print(f"\n\n\nThe number of matches is {num_matches}\n\n")

task = input("Choose which task to run:\n1- Task I\n2- Task II\n3- Task III\n4- Task IV\n5- Task V\n6- All tasks\nYour input: ")
if not task.isnumeric():
    print("Invalid input\nexiting....")
    exit()
task = int(task)
if task > 6:
    print("Invalid input\nexiting....")
    exit()

print_mode = input(
    "Choose the number of items to be printed:\n1- 10 items\n2- 20 items\n3- Custom number\nInput: ")
if not print_mode.isnumeric():
    print("Invalid input\nexiting....")
    exit()
print_mode = int(print_mode)
if print_mode > 3:
    print("Invalid input\nexiting....")
    exit()
print_num = "20"
if print_mode == 3:
    print_num = input("Enter your value: ")
    if not print_num.isnumeric():
        print("Invalid input\nexiting....")
        exit()
    print_num = int(print_num)
elif print_mode == 1:
    print_num = 10
else:
    print_num = 20


items = get_data(ITEMS_TABLE, spark)

names = get_data(ITEMS_NAMES_TABLE, spark)

champs = get_data(CHAMPIONS_TABLE, spark)

if task == 1:
    c_win, c_pick, c_ban = get_task_I(champs)

    print("\n\nChampions Wins")
    c_win.show(print_num)

    print("\n\nChampions Pick")
    c_pick.show(print_num)

    print("\n\nChampions Ban")
    c_ban.show(print_num)
elif task == 2:
    c_syn = get_task_II(champs)

    print("\n\nChampions synergy (win/duo)")
    c_syn.show(print_num)
elif task == 3:
    i_wins, i_picks = get_task_III(champs, items, names)

    print("\n\nItems Wins")
    i_wins.show(print_num)

    print("\n\nItems Pick")
    i_picks.show(print_num)
elif task == 4:
    champ_analysis, class_analysis = get_task_IV(champs, items, names)

    print("\n\nItems Synergy based on champion")
    champ_analysis.show(print_num)

    print("\n\nItems Synergy based on class")
    class_analysis.show(print_num)
elif task == 5:
    champ_analysis, class_analysis = get_task_IV(champs, items, names)

    chosen_champs, chosen_items = get_task_V(champs, champ_analysis, spark)

    print("\n\nItems suggestions for the following champions")
    chosen_champs.show(print_num)
    print("\n\n are:")
    chosen_items.show(print_num)
elif task == 6:
    c_win, c_pick, c_ban = get_task_I(champs)

    print("\n\nChampions Wins")
    c_win.show(print_num)

    print("\n\nChampions Pick")
    c_pick.show(print_num)

    print("\n\nChampions Ban")
    c_ban.show(print_num)

    c_syn = get_task_II(champs)

    print("\n\nChampions synergy (win/duo)")
    c_syn.show(print_num)
    i_wins, i_picks = get_task_III(champs, items, names)

    print("\n\nItems Wins")
    i_wins.show(print_num)

    print("\n\nItems Pick")
    i_picks.show(print_num)

    champ_analysis, class_analysis = get_task_IV(champs, items, names)

    print("\n\nItems Synergy based on champion")
    champ_analysis.show(print_num)

    print("\n\nItems Synergy based on class")
    class_analysis.show(print_num)

    chosen_champs, chosen_items = get_task_V(champs, champ_analysis, spark)

    print("\n\nItems suggestions for the following champions")
    chosen_champs.show(print_num)
    print("\n\n are:")
    chosen_items.show(print_num)
