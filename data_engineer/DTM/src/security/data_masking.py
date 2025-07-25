from pyspark.sql import DataFrame, functions as F
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
from config import Config

class DataMasker:
    def __init__(self):
        self.ENCRYPTION_KEY = Config.ENCRYPTION_KEY

    def mask_data(self, df: DataFrame, column: str) -> DataFrame:
        """Aplica técnicas de mascaramento de dados conforme LGPD"""

        if column == "cpf":
            # Exibe apenas os caracteres do meio
            return df.withColumn(
                column,
                F.when(
                    F.col(column).isNotNull(),
                    F.concat(F.lit("***"), F.substring(column, 4, 3), F.lit("***"))
                ).otherwise(F.col(column))
            )

        elif column in {"patient_id", "medical_record_number"}:
            # Mascaramento via hash com chave
            return df.withColumn(
                column,
                F.sha2(F.concat(F.col(column), F.lit(self.ENCRYPTION_KEY)), 256)
            )

        else:
            # Regra genérica — manter prefixo e substituir restante
            return df.withColumn(
                column,
                F.when(
                    F.col(column).isNotNull(),
                    F.regexp_replace(column, r"(^.{3}).*", r"$1***")
                ).otherwise(F.col(column))
            )
