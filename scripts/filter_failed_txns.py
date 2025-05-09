from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("FilterFailedTxns").getOrCreate()

df = spark.read.option("header", True).csv("gs://banking_transaction/cleaned/merged_data.csv")

failed_df = df.filter(col("status") == "FAILED")

# Save failed transactions to GCS for import to Cloud SQL
failed_df.coalesce(1).write.mode("overwrite").option("header", True).csv("gs://banking_transaction/final/failed_txns.csv")

spark.stop()
