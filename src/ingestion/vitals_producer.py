# =============================================================================
# MedStream 360 – Day 2: Kafka Producer for Real-Time Vitals Simulation
# =============================================================================
import pandas as pd
from kafka import KafkaProducer
import json
from kafka.errors import KafkaError
import random
import time
import logging
import os
from pathlib import Path


# setup logging
logging.basicConfig(level = logging.INFO,
                    format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Correct path – assuming file is in data/raw/icu/
BASE_DIR = Path(__file__).resolve().parents[2]
vitals_path = BASE_DIR / "data/raw/icu/chartevents.csv"
try:
    df = pd.read_csv(vitals_path, nrows=50000)  # limit to avoid memory issues
    logger.info(f"Loaded {len(df):,} vital records from MIMIC-IV Demo")
    print(df.head())  # Quick preview
    print(df.info())  # Column types & non-null counts
except FileNotFoundError:
    logger.error(f"Dataset not found at {vitals_path}")
    logger.error("Please place 'chartevents.csv' exactly in: data/raw/icu/")
    logger.error("Current working directory: " + os.getcwd())
    exit(1)
except Exception as e:
    logger.error(f"Unexpected error loading file: {e}")
    exit(1)

# Filter meaningful vitals
key_vitals = [220045,  # Heart Rate
              225664,  # Glucose
              220181,  # Non-Invasive BP Systolic
              220179,  # Respiratory Rate
              220050]  # Arterial BP Systolic
df_vitals = df[df['itemid'].isin(key_vitals)]

#kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = lambda v:json.dumps(v).encode('utf-8'),
    retries = 5, # for production reliability
    acks='all' # wait for all replicas
)

topic = 'patient-vitals'
logger.info(f"starting real-time vitals stream to topic '{topic}' .... Press Ctrl+C to stop")

sent_count = 0 
try:
    for _, row in df_vitals.iterrows():
        message = {
            'subject_id': int(row['subject_id']),
            'hadm_id': int(row['hadm_id']),
            'stay_id': int(row['stay_id']),
            'charttime': str(row['charttime']),
            'itemid': int(row['itemid']),
            'value': float(row['valuenum']) if pd.notna(row['valuenum']) else None,
            'valueuom': row['valueuom'],
            'storetime': str(row['storetime']) if pd.notna(row['storetime']) else None
        }

        try:
            producer.send(topic, value=message)
            sent_count +=1
            if sent_count % 100 == 0: 
                logger.info(f"sent{sent_count} messages ....")
        except KafkaError as e:
            logger.error(f"Failed to send message: {e}")

        # simulating realistsic delay
        time.sleep(random.uniform(0.5,3.0))

except KeyboardInterrupt:
    logger.info("streaming interrupted by user.")
except Exception as e:
    logger.error(f"An error occurred: {e}")

finally:
    producer.flush()
    producer.close()
    logger.info(f"Total messages sent: {sent_count}")