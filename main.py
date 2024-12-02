import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, avg, min


def print_formatted_output(batch_df, batch_id):
    results = batch_df.collect()
    for row in results:
        print(f"{row['class']}: {row['average_score']:.1f}\n")


spark = SparkSession.builder.appName("Test").getOrCreate()

schema = "timestamp INT, class STRING, score FLOAT"

data = spark.readStream.schema(schema).json("./data")

averages = data.groupBy(
    window(col("timestamp").cast("timestamp"), "10 seconds"),
    col("class")
).agg(
    avg("score").alias("average_score"),
    min("timestamp").alias("min_timestamp"),
).orderBy(
    "window",
    "min_timestamp"
).select(
    col("class"),
    col("average_score")
)

query = averages.writeStream.outputMode("complete").foreachBatch(print_formatted_output).start()

query.awaitTermination()
