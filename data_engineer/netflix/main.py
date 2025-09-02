import sys
import os
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.settings import Config
from src.data_ingestion.extractorLoad import BatchExtractLoad
from src.data_processing.data_transformation import DataTransformer
from src.data_storage.data_lake import DataLakeManager
from src.data_quality.data_quality import DataQuality


class DataPipelineNetflix:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .master(Config.SPARK_MASTER) \
            .config("spark.executor.memory", Config.SPARK_EXECUTOR_MEMORY) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
            .config("spark.pyspark.python", "/opt/homebrew/bin/python3.9") \
            .config("spark.pyspark.driver.python", "/opt/homebrew/bin/python3.9") \
            .getOrCreate()
        
    def run_batch_pipeline(self,arq):

        # Data Processing
        transf = DataTransformer()
        df_transformed = transf.apply_transformations(arq)
        dfs = transf.country_transformations(df_transformed)

        # Data Storage
        storage = DataLakeManager(self.spark)
        storage.save_to_data_lake(dfs, "arq/")
        storage.save_to_postgres(
            {
            "tb_netflix": df_transformed,
            "tb_country_netflix": dfs
            }
        )
        # Data Ingestion
        extractor = BatchExtractLoad(self.spark)
        df = extractor.from_database("SELECT * FROM tb_score_netflix")
        # Data Quality
        dq = DataQuality(self.spark)
        quality_report = dq.check_frequency_all(df, threshold=1)
        dfDQ = quality_report["*"]

        # Save Data Quality Report
        storage.save_to_postgres({"data_quality_report": dfDQ})


    def run(self):
        try:
            arq = self.spark.read.option("delimiter", ",").option("header", True).csv('/Users/eduardoalberto/LoadFile/input/netflix_titles_clean.csv',inferSchema=True)
            self.run_batch_pipeline(arq)
        except KeyboardInterrupt:
            print("Pipeline interrompido pelo usuário")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    pipeline = DataPipelineNetflix()
    pipeline.run()

