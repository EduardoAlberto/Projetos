import sys
import os
from pathlib import Path
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.settings import Config
from src.data_ingestion.batch.extractors import BatchDataExtractor
from src.data_ingestion.streaming.stream_processor import StreamProcessor
from src.data_processing.batch_transformations import DataTransformer
from src.security.access_control import AccessController
from src.monitoring.metrics import PipelineMetrics
from src.data_storage.data_lake import DataLakeManager


class HealthcareDataPipeline:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .master(Config.SPARK_MASTER) \
            .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
            .config("spark.pyspark.python", "/opt/homebrew/bin/python3.9") \
            .config("spark.pyspark.driver.python", "/opt/homebrew/bin/python3.9") \
            .getOrCreate()

        self.transformer = DataTransformer()
        self.access_controller = AccessController(self.spark)
        self.metrics = PipelineMetrics(self.spark)
        self.storage = DataLakeManager(self.spark)

    def run_batch_pipeline(self):
        extractor = BatchDataExtractor(self.spark)
        df = extractor.from_database("SELECT * FROM healthcare_records")

        df = self.transformer.apply_batch_transformations(df)
        df = self.transformer.transform_healthcare_data(df)
        df = self.metrics.add_metrics(df)
        df = df.withColumn("date", to_date("timestamp"))\
               .withColumn("cpf", lit(None).cast("string"))\
               .withColumn("processing_time", current_timestamp())

        self.storage.save_to_data_lake(df, "healthcare/processed", ["date"])
        extractor.save_to_postgres(df)

    def run_streaming_pipeline_fake(self):
        processor = StreamProcessor(self.spark)
        df = processor.generate_fake_healthcare_stream()

        df_transformed = self.transformer.transform_healthcare_data(df)
        df_metrics = self.metrics.add_metrics(df_transformed)

        df_metrics = df_metrics.withColumn("date", to_date("timestamp")) \
                            .withColumn("processing_time", current_timestamp())  # <-- ADICIONADO

        self.storage.save_to_data_lake(df_metrics, "healthcare/streaming", ["date"])

        extractor = BatchDataExtractor(self.spark)
        extractor.save_to_postgres(df_metrics)


    def run(self):
        try:
            self.run_batch_pipeline()
            self.run_streaming_pipeline_fake()
        except KeyboardInterrupt:
            print("Pipeline interrompido pelo usuário")
        finally:
            self.spark.stop()


if __name__ == "__main__":
    pipeline = HealthcareDataPipeline()
    pipeline.run()
