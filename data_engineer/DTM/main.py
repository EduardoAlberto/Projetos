import sys
import os
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, current_timestamp  # ✅
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType  # ✅

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
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
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
        df = df.withColumn("date", to_date("timestamp"))
        self.storage.save_to_data_lake(df, "healthcare/processed", ["date"])
    
    def run_streaming_pipeline_fake(self):
        # ✅ Simulação de dados de streaming
        schema = StructType([
            StructField("patient_id", StringType()),
            StructField("timestamp", TimestampType()),
            StructField("blood_pressure", StringType()),
            StructField("heart_rate", IntegerType())
        ])
        
        # ✅ Dados fictícios
        data = [("p1", None, "120/80", 75)]
        df = self.spark.createDataFrame(data, schema)
        
        # ✅ Timestamp atual para simular streaming
        df = df.withColumn("timestamp", current_timestamp())
        df = df.withColumn("date", to_date("timestamp"))

        df = self.transformer.apply_batch_transformations(df)
        df = self.transformer.transform_healthcare_data(df)
        df = self.metrics.add_metrics(df)

        # ✅ Salvar como se fosse streaming
        self.storage.save_to_data_lake(df, "healthcare/real_time_simulated", ["date"])
        print("Simulação de streaming executada com sucesso.")
    
    def run(self):
        try:
            self.run_batch_pipeline()
            self.run_streaming_pipeline_fake()  # ✅ Substitui o método original com Kafka
        except KeyboardInterrupt:
            print("Pipeline interrompido pelo usuário")
        finally:
            self.spark.stop()


if __name__ == "__main__":
    pipeline = HealthcareDataPipeline()
    pipeline.run()
