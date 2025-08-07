from typing import Dict
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../../..')))
from pyspark.sql import DataFrame
import yfinance as yf
import warnings
from config.settings import Config

warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")

class BatchExtractLoad:
    def __init__(self, spark):
        self.spark = spark

    def from_database(self, query: str) -> DataFrame:
            return self.spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/dev5114")\
                .option("query", query) \
                .option("user", os.getenv("DB_USER", "postgres")) \
                .option("password", os.getenv("DB_PASSWORD", "postgre123")) \
                .option("driver", "org.postgresql.Driver") \
                .load()

    def from_file(self, path: str, file_type: str = "csv") -> DataFrame:
        if file_type == "csv":
            return self.spark.read.csv(path, header=True, inferSchema=True)
        elif file_type == "parquet":
            return self.spark.read.parquet(path)
        else:
            raise ValueError(f"Tipo de arquivo não suportado: {file_type}")
        
    def save_to_postgres(self, df):
        jdbc_url = Config.DATA_SOURCES["jdbc_url"]
        props = {
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "postgre123"),
            "driver": "org.postgresql.Driver"
        }

        df.write \
            .mode("append") \
            .jdbc(url=jdbc_url, table="healthcare_real_time", properties=props)

        print("Dados simulados salvos no PostgreSQL com sucesso.")