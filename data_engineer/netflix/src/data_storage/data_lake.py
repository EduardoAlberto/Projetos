import sys
import os
from config.settings import Config
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pyspark.sql import DataFrame
from config import settings
from pyspark.sql import functions as F

class DataLakeManager:
    def __init__(self, spark):
        self.spark = spark

    def save_to_data_lake(self, df: DataFrame, path: str) -> None:
        full_path = f"{settings.Config.STORAGE['data_lake_path']}/{path}"

        df_with_metadata = df.withColumn("data_execucao", F.current_timestamp())

        df_with_metadata.write.mode("overwrite").parquet(full_path)

    def save_to_postgres(self, df):
        jdbc_url = Config.DATA_SOURCES["jdbc_url"]
        props = {
            "user": os.getenv("DB_USER", "dbpostgres"),
            "password": os.getenv("DB_PASSWORD", "postgre123"),
            "driver": "org.postgresql.Driver"
        }

        df.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="healthcare_real_time", properties=props)

        print("Dados simulados salvos no PostgreSQL com sucesso.")

