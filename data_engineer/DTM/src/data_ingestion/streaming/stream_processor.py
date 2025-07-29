import sys
import os
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *

# Ajusta caminho para importar config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from config import settings


class StreamProcessor:
    def __init__(self, spark):
        self.spark = spark

    def from_kafka(self, topic: str) -> DataFrame:
        """
        Lê stream do Kafka e retorna DataFrame estruturado.
        """
        raw_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", settings.Config.DATA_SOURCES["kafka_brokers"]) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()

        # Extrai timestamp e valor, converte valor JSON para colunas
        json_df = raw_df.selectExpr(
            "CAST(timestamp AS TIMESTAMP) as event_time",
            "CAST(value AS STRING) as json_data"
        )

        structured_df = json_df.select(
            "event_time",
            F.from_json("json_data", self._get_schema()).alias("data")
        ).select("event_time", "data.*")

        return structured_df

    def _get_schema(self) -> StructType:
        """
        Retorna o schema esperado para os dados do streaming.
        """
        return StructType([
            StructField("patient_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("blood_pressure", StringType(), True),
            StructField("heart_rate", IntegerType(), True)
        ])

    def process_healthcare_stream(self, stream_df: DataFrame) -> DataFrame:
        """
        Método placeholder para aplicar transformações no stream.
        """
        # Aqui você pode aplicar transformações específicas, ex:
        # return stream_df.withColumn("new_col", F.lit(1))
        return stream_df

    def generate_fake_healthcare_stream(self) -> DataFrame:
        """
        Gera um DataFrame simulado para testes de streaming.
        """
        from pyspark.sql.functions import current_timestamp, lit

        # Cria dados fake com timestamp atual
        data = [
            ("12345", "120/80", 72),
            ("67890", "130/85", 75)
        ]

        schema = StructType([
            StructField("patient_id", StringType(), True),
            StructField("blood_pressure", StringType(), True),
            StructField("heart_rate", IntegerType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        # Adiciona coluna timestamp simulando chegada de dados em tempo real
        df = df.withColumn("timestamp", current_timestamp())

        return df
