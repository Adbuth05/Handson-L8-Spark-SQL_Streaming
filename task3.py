# task3.py  — Windowed time-based analytics


import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, sum as _sum
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main(host: str, port: int):
    # Spark session (2 local threads; fewer shuffle partitions for small labs)
    spark = (SparkSession.builder
             .appName("RideSharingAnalytics-Task3")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())

    # Schema matches the generator.py
    schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("driver_id", IntegerType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("timestamp", StringType(), True)   # will cast below
    ])

    # 1) Read lines from socket
    raw = (spark.readStream
                 .format("socket")
                 .option("host", host)
                 .option("port", port)
                 .load())

    # 2) Parse JSON into columns
    parsed = (raw.select(from_json(col("value"), schema).alias("data"))
                  .select("data.*"))

    # 3) Cast timestamp -> TimestampType and add watermark (event time)
    with_time = (parsed
                 .withColumn("event_time",
                             to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
                 .withWatermark("event_time", "1 minute"))

    # 4) Windowed aggregation: 5-minute window, sliding 1 minute; sum of fare_amount
    win_agg = (with_time
               .groupBy(window(col("event_time"), "5 minutes", "1 minute"))
               .agg(_sum("fare_amount").alias("total_fare")))

    # 5) Flatten window struct to start/end columns for nicer CSVs
    result = (win_agg
              .select(
                  col("window.start").alias("window_start"),
                  col("window.end").alias("window_end"),
                  col("total_fare")
              ))

    # 6) Write windowed results to CSV (append mode; finalized windows only)
    query = (result.writeStream
             .format("csv")
             .outputMode("append")  # window+watermark -> append finalized windows
             .option("path", "outputs/task3")
             .option("checkpointLocation", "checkpoints/task3")
             .option("header", "true")
             .trigger(processingTime="30 seconds")
             .start())

    query.awaitTermination()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9999)
    args = ap.parse_args()
    main(args.host, args.port)
