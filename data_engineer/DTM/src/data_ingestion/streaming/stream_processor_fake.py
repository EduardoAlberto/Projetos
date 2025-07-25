from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def run_streaming_pipeline_fake(self):
    schema = StructType([
        StructField("patient_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("blood_pressure", StringType()),
        StructField("heart_rate", IntegerType())
    ])
    
    # Criar DataFrame estático com dados de exemplo
    data = [("p1", None, "120/80", 70)]
    df = self.spark.createDataFrame(data, schema)
    
    # Adiciona coluna de timestamp atual para simular stream
    df = df.withColumn("timestamp", current_timestamp())
    
    # Aplica as transformações e métricas
    df = self.transformer.apply_batch_transformations(df)
    df = self.transformer.transform_healthcare_data(df)
    df = self.metrics.add_metrics(df)
    
    # Salva como batch para teste
    self.storage.save_to_data_lake(df, "healthcare/processed_fake", ["date"])
    
    print("Streaming fake executado (na verdade batch de teste).")


