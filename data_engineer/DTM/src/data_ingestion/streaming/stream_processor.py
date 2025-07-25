import sys
import os
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *

# Adiciona o caminho raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from config import settings


class StreamProcessor:
    def __init__(self, spark):
        self.spark = spark

    def from_kafka(self, topic: str) -> DataFrame:
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.Config.DATA_SOURCES["kafka_brokers"]) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load() \
            .selectExpr("CAST(timestamp AS TIMESTAMP) as event_time", 
                        "CAST(value AS STRING) as json_data") \
            .select(
                "event_time",
                F.from_json("json_data", self._get_schema()).alias("data")
            ) \
            .select("event_time", "data.*")

    def _get_schema(self) -> StructType:
        return StructType([
            StructField("patient_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("blood_pressure", StringType(), True),
            StructField("heart_rate", IntegerType(), True)
        ])

    def process_healthcare_stream(self, stream_df: DataFrame) -> DataFrame:
        return stream_df
