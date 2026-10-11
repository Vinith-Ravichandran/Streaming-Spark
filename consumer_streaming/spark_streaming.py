import os
import sys

# Windows environment setup
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"
if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from schemas import TRANSACTION_SCHEMA

# 1. Initialize Spark Session
spark = (
    SparkSession.builder.appName("RetailStreamAnalytics")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )
    .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
    .config(
        "spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin"
    )
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)

# 2. Windows NativeIO workaround
try:
    jvm = spark._jvm
    nativeIO = jvm.org.apache.hadoop.io.nativeio.NativeIO
    field = nativeIO.Windows.getClass().getDeclaredField(
        "usePrivilegedDataTransfer"
    )
    field.setAccessible(True)
    field.setBoolean(None, False)
except Exception:
    pass

spark.sparkContext.setLogLevel("WARN")
print("--> Spark initialized. Listening to Kafka stream on localhost:9092...")

# 3. Read Stream from Kafka
kafka_raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "retail-transactions")
    .option("startingOffsets", "latest")
    .load()
)

# 4. Parse JSON & Filter
parsed_stream = (
    kafka_raw_stream.selectExpr("CAST(value AS STRING) AS json_payload")
    .select(
        F.from_json(F.col("json_payload"), TRANSACTION_SCHEMA).alias("data")
    )
    .select("data.*")
    .filter((F.col("status") == "SUCCESS") & (F.col("amount") > 0))
)

# 5. Tumbling Window Aggregation with Watermark
windowed_summary = (
    parsed_stream.withWatermark("event_time", "30 seconds")
    .groupBy(
        F.window(F.col("event_time"), "1 minute"),
        F.col("category"),
    )
    .agg(
        F.round(F.sum("amount"), 2).alias("total_sales"),
        F.count("transaction_id").alias("order_count"),
        F.round(F.avg("amount"), 2).alias("avg_order_value"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("category"),
        F.col("total_sales"),
        F.col("order_count"),
        F.col("avg_order_value"),
    )
)

# 6. Stream Sink to Console
CHECKPOINT_PATH = r"C:\hadoop\checkpoints\spark_console"

query = (
    windowed_summary.writeStream.outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

print("--> Streaming query active. Waiting for batches...")
query.awaitTermination()