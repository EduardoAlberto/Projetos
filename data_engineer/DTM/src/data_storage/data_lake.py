import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pyspark.sql import DataFrame
from config import settings

class DataLakeManager:
    def __init__(self, spark):
        self.spark = spark

    def save_to_data_lake(self, df: DataFrame, path: str, partition_cols: list = None) -> None:
        full_path = f"{settings.Config.STORAGE['data_lake_path']}/{path}"
        writer = df.write.mode("overwrite")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)  # <-- Correção aqui

        writer.parquet(full_path)

    def stream_to_data_lake(self, stream_df: DataFrame, path: str, partition_cols: list = None):
        full_path = f"{settings.Config.STORAGE['data_lake_path']}/{path}"
        writer = stream_df.writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", full_path) \
            .option("checkpointLocation", "/tmp/checkpoints")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)  # <-- Correção aqui

        return writer.start()
