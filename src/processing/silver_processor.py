# =============================================================================
# MedStream 360 – Silver Layer Processing (FINAL CLEAN VERSION)
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, to_timestamp, current_timestamp
)
from delta import configure_spark_with_delta_pip
import logging
import os

# ---------------- Logging ----------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------- Spark Session ----------------
builder = SparkSession.builder \
    .appName("MedStream360 - Silver Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ---------------- Paths (IMPORTANT FIX) ----------------
BASE_DIR = os.path.abspath(".")
bronze_path = os.path.join(BASE_DIR, "data/bronze/vitals")
silver_path = os.path.join(BASE_DIR, "data/silver/vitals")

logger.info("🚀 Reading from Bronze layer...")

# ---------------- Read Bronze ----------------
df = spark.read.format("delta").load(bronze_path)

logger.info(f"Bronze records: {df.count()}")

# ---------------- Transformations ----------------
df_silver = df \
    .withColumn("charttime", to_timestamp(col("charttime"))) \
    .withColumn("storetime", to_timestamp(col("storetime"))) \
    .withColumn("value", col("value").cast("double")) \
    .withColumn("processed_time", current_timestamp())

# ---------------- Vital Type Mapping ----------------
df_silver = df_silver.withColumn(
    "vital_type",
    when(col("itemid") == 220045, "heart_rate")
    .when(col("itemid") == 225664, "glucose")
    .when(col("itemid") == 220181, "blood_pressure_systolic")
    .when(col("itemid") == 220179, "respiratory_rate")
    .otherwise("other")
)

# ---------------- Abnormal Flag ----------------
df_silver = df_silver.withColumn(
    "is_abnormal",
    when((col("vital_type") == "heart_rate") &
         ((col("value") < 50) | (col("value") > 120)), 1)
    .when((col("vital_type") == "glucose") &
          (col("value") > 200), 1)
    .when((col("vital_type") == "blood_pressure_systolic") &
          (col("value") > 140), 1)
    .when((col("vital_type") == "respiratory_rate") &
          ((col("value") < 8) | (col("value") > 30)), 1)
    .otherwise(0)
)

# ---------------- Clean Data ----------------
df_silver = df_silver.dropna(subset=["value", "charttime"])

# ---------------- Write Silver ----------------
df_silver.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("vital_type") \
    .save(silver_path)

logger.info("✅ Silver layer written successfully")

# ---------------- Preview ----------------
df_silver.select(
    "subject_id",
    "charttime",
    "vital_type",
    "value",
    "is_abnormal"
).show(10, False)