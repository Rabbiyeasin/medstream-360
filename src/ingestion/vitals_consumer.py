# =============================================================================
# MedStream 360 – Day 3: Kafka Consumer + Bronze Layer Landing
# =============================================================================

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json,current_timestamp
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType,TimestampType
import logging
import json
import os 

# setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Spark Session with Delta Lake
spark = SparkSession.builder \
    .appName("MedStream360 - Bronze Layer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

#kafka consumer
consumer = KafkaConsumer(
    'patient_vitals',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='bronze-consumer-group',
    value_deserializer = lambda m: json.loads(m.decode('utf-8'))
)

#Bronze Storage path
bronze_path = "data/bronze/vitals"

#Schema for incoming messages(enforced)
schema = StructType([
    StructField("subject_id",IntegerType(),True),
    StructField("hadm_id",IntegerType(),True),
    StructField("stay_id",IntegerType(),True),
    StructField("charttime",TimestampType(),True),
    StructField("itemid",IntegerType(),True),
    StructField("value",DoubleType(),True),
    StructField("valueuom",StringType(),True),
    StructField("storetime",TimestampType(),True)
])

logger.info("Starting Kafka consumer -> Bronze Layer writer ... Press Ctrl+C to stop")

processed_count = 0 
batch =[]

try:
    for message in consumer:
        try:
            data = message.value
            batch.append(data)
            processed_count += 1 

            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} messages.")

                # Every 500 messages -> write batch to Bronze Layer
                if len(batch) >= 500:
                    df_batch = spark.createDataFrame(batch,schema =schema)
                    df_batch = df_batch.withColumn("ingestion_time",current_timestamp())

                    # write as Delta table (append mode)
                    df_batch.write.format("delta")\
                        .mode("append")\
                        .partitionBy("chattime")\
                        .save(bronze_path)
                    
                    logger.info(f"write batch of {len(batch)} records to Bronze Delta table")
                    batch =[]

        except Exception as e:
            logger.error(f"Error processing message: {e}")

except KeyboardInterrupt:
    logger.info("Consumer stopped by user")
except KafkaError as e:
    logger.error(f"Kafka error: {e}")  
finally:
    consumer.close()
    spark.stop()
    logger.info(f"Total messages processed: {processed_count}")
    