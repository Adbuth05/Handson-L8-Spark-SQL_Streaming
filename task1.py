# task1.py  — Ingest + parse JSON -> write CSV only

import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

def main(host: str, port: int):
    # Spark session (2 local threads; fewer shuffle partitions for a small lab)
    spark = (
        SparkSession.builder
        .appName("RideSharingAnalytics-Task1")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    
    schema = StructType([
        StructField("trip_id", StringType(), True),
        StructField("driver_id", IntegerType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("timestamp", StringType(), True)   # cast to Timestamp later in Task 3
    ])

    # Read lines from the socket
    raw = (
        spark.readStream
        .format("socket")
        .option("host", host)
        .option("port", port)
        .load()
    )

    # Parse JSON into columns
    parsed = (
        raw.select(from_json(col("value"), schema).alias("data"))
           .select("data.*")
    )

    # Output locations (absolute paths so there's no confusion about cwd)
    out_dir  = str((Path.cwd() / "outputs" / "task1").resolve())
    ckpt_dir = str((Path.cwd() / "checkpoints" / "task1").resolve())

    # Write ONLY to CSV (no console sink)
    query = (
        parsed.writeStream
        .format("csv")
        .outputMode("append")
        .option("path", out_dir)
        .option("checkpointLocation", ckpt_dir)
        .option("header", "true")
        .trigger(processingTime="10 seconds")  # write a batch every 10s
        .start()
    )

    print(f"[Task1] Writing CSVs to: {out_dir}")
    print(f"[Task1] Checkpoint at:   {ckpt_dir}")
    query.awaitTermination()

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9999)
    args = ap.parse_args()
    main(args.host, args.port)
