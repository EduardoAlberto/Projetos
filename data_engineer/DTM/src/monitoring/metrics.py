# monitoring/metrics.py
from pyspark.sql import DataFrame, functions as F

class PipelineMetrics:
    def __init__(self, spark):
        self.spark = spark
    
    def add_metrics(self, df: DataFrame) -> DataFrame:
        return df \
            .withColumn("processing_lag_ms", 
                       (F.current_timestamp().cast("long") - F.col("timestamp").cast("long")) * 1000) \
            .withColumn("record_size_bytes", 
                       F.length(F.to_json(F.struct([F.col(c) for c in df.columns]))))
    
    def monitor_data_quality(self, df: DataFrame):
        missing_data = df.agg(
            *[F.sum(F.when(F.isnull(c), 1).otherwise(0)).alias(c) for c in df.columns]
        )
        stats = df.describe()
        
        print("Dados faltantes por coluna:")
        missing_data.show()
        print("\nEstatísticas descritivas:")
        stats.show()
    
    def send_alert(self, message: str, severity: str = "warning"):
        print(f"[{severity.upper()}] {message}")