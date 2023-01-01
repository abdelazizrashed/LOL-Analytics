
from pyspark.sql import SparkSession, DataFrame


def print_df_info(df: DataFrame):
    df.show()
    df.printSchema()
    print(f"DataFrame count ===>>> {df.count()}")
