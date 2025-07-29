from pyspark.sql.functions import current_timestamp, datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def run_streaming_pipeline_fake(self):
    schema = StructType([
        StructField("patient_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("blood_pressure", StringType()),
        StructField("heart_rate", IntegerType())
    ])

    # Dados simulados
    data = [
        ("12345", datetime.now(), "120/80", 72),
        ("67890", datetime.now(), "130/85", 75)
    ]

    df = self.spark.createDataFrame(data, schema=schema)
    df = df.withColumn("timestamp", current_timestamp())
    df = df.withColumn("date", to_date("timestamp"))

    df = self.transformer.apply_batch_transformations(df)
    df = self.transformer.transform_healthcare_data(df)
    df = self.metrics.add_metrics(df)

    # ✅ Salva no Data Lake
    self.storage.save_to_data_lake(df, "healthcare/real_time_simulated", ["date"])

    # ✅ Salva no banco de dados PostgreSQL
    self.save_to_postgres(df)

    print("Simulação de streaming executada com sucesso.")
