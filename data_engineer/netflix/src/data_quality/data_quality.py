from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from functools import reduce

class DataQuality:
    def __init__(self, spark):
        self.spark = spark

    def check_frequency_all(self, df: DataFrame, threshold: int = 1) -> dict:
        """
        Verifica a frequência de todas as colunas de um DataFrame e retorna consolidado.

        Args:
            df (DataFrame): DataFrame a ser analisado
            threshold (int): frequência mínima esperada para cada valor

        Returns:
            dict: {"*": DataFrame com frequências consolidadas}
        """
        freq_dfs = []

        for col in df.columns:
            freq_df = (
                df.groupBy(F.col(col).cast("string").alias("valor")) 
                  .count()
                  .withColumn("coluna", F.lit(col))
                  .withColumn(
                      "quality_flag",
                      F.when(F.col("count") >= threshold, F.lit("OK"))
                       .otherwise(F.lit("FREQUÊNCIA BAIXA"))
                  )
                  .select("coluna", "valor", "count", "quality_flag")
            )
            freq_dfs.append(freq_df)

        # União de todos os DataFrames de frequência
        consolidated = reduce(lambda a, b: a.unionByName(b), freq_dfs)

        return {"*": consolidated}
