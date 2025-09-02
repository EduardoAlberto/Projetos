from typing import Dict
import requests
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.getcwd(), '../../..')))
from pyspark.sql import DataFrame
import warnings


warnings.filterwarnings("ignore", message=".*NotOpenSSLWarning.*")

class BatchExtractLoad:
    def __init__(self, spark):
        self.spark = spark

    def from_database(self, query: str) -> DataFrame:
            return self.spark.read \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://localhost:5432/dbpostgres")\
                .option("query", query) \
                .option("user", os.getenv("DB_USER", "dbpostgres")) \
                .option("password", os.getenv("DB_PASSWORD", "postgre123")) \
                .option("driver", "org.postgresql.Driver") \
                .load()

        
