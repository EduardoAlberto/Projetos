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

        df_with_metadata = df.withColumn("data_execucao", F.current_date())

        df_with_metadata.write.mode("overwrite").partitionBy("data_execucao").parquet(full_path)

    def save_to_postgres(self, tables: dict):
        """
        Salva múltiplos DataFrames em tabelas no PostgreSQL.
        
        Args:
            tables (dict): dicionário no formato {"nome_tabela": DataFrame, ...}
        """
        jdbc_url = Config.DATA_SOURCES["jdbc_url"]
        props = {
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgre123"),
            "driver": "org.postgresql.Driver"
        }

        for table_name, df in tables.items():
            print(f"Salvando dados na tabela {table_name}...")
            df.write \
                .mode("overwrite") \
                .jdbc(url=jdbc_url, table=table_name, properties=props)
            print(f"Tabela {table_name} salva com sucesso no PostgreSQL.")


            

