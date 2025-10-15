from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg as _avg, sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1) Create a Spark session
spark = (SparkSession.builder
         .appName("RideSharingAnalytics-Task2")
         .getOrCreate())

# 2) Define the schema for incoming JSON data (matches your generator.py)
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),   # <-- IMPORTANT: integer
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)     # keep as string; Task 3 will cast
])

# 3) Read streaming data from socket
raw = (spark.readStream
            .format("socket")
            .option("host", "localhost")
            .option("port", 9999)
            .load())

# 4) Parse JSON data into columns using the defined schema
df = (raw.select(from_json(col("value"), schema).alias("data"))
          .select("data.*"))

# 5) Compute aggregations: total fare and average distance grouped by driver_id
agg = (df.groupBy("driver_id")
         .agg(
             _sum("fare_amount").alias("total_fare"),
             _avg("distance_km").alias("avg_distance")
         ))

# 6) Define a function to write each micro-batch to a CSV (one file per batch)
def write_batch(batch_df, batch_id: int):
    (batch_df
        .coalesce(1)  # single CSV per batch folder
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"outputs/task2/batch={batch_id:05d}"))

# 7) Use foreachBatch to apply the function to each micro-batch
query = (agg.writeStream
            .outputMode("complete")  # required for non-windowed aggregations
            .option("checkpointLocation", "checkpoints/task2")
            .foreachBatch(write_batch)
            .trigger(processingTime="10 seconds")
            .start())

query.awaitTermination()
