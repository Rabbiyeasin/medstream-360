import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MediStream360").getOrCreate()

data = [
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 35)
]

df = spark.createDataFrame(data, ["Name", "Age"])

df.show()

spark.stop()