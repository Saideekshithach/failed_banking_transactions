from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("CleanMergeBankData").getOrCreate()

df = spark.read.option("header", True).csv("gs://banking_transaction/injection/*.csv")

# Drop nulls and blanks
df_cleaned = df.dropna()
for column in df_cleaned.columns:
    df_cleaned = df_cleaned.filter(col(column).isNotNull() & (col(column) != ''))

# Save as single merged cleaned CSV
df_cleaned.coalesce(1).write.mode("overwrite").option("header", True).csv("gs://banking_transaction/cleaned/merged_data.csv")

spark.stop()

