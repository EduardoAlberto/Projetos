import sys
import os
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.settings import Config
from src.data_ingestion.batch.extractorLoad import BatchExtractLoad
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))


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
        
    def run_batch_pipeline(self):
        extractor = BatchExtractLoad(self.spark)
        df = extractor.from_database("SELECT * FROM public.tb_netflix")

        df.show()

        # self.storage.save_to_data_lake(df, "healthcare/processed", ["date"])
        # extractor.save_to_postgres(df)

    def run(self):
        try:
            self.run_batch_pipeline()
        except KeyboardInterrupt:
            print("Pipeline interrompido pelo usuário")
        finally:
            self.spark.stop()

if __name__ == "__main__":
    pipeline = DataPipelineNetflix()
    pipeline.run()

