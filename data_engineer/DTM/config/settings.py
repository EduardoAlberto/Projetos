import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Configurações do Spark
    SPARK_MASTER = "local[*]"
    SPARK_APP_NAME = "HealthcareDataPipeline"
    SPARK_EXECUTOR_MEMORY = "2g"

    # Fontes de dados
    DATA_SOURCES = {
        "government_api": "https://dummyjson.com/user/2",
        "kafka_brokers": os.getenv("KAFKA_BROKERS", "localhost:9092"),
        "jdbc_url": os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/healthcare")
    }

    # Armazenamento
    STORAGE = {
        "data_lake_path": os.getenv("DATA_LAKE_PATH", "/Users/eduardoalberto/LoadFile/output/healthcare/processed"),
        "warehouse_path": os.getenv("WAREHOUSE_PATH", "/Users/eduardoalberto/LoadFile/output/healthcare/processed")
    }

    # Segurança
    ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", "dummy_key")
    SENSITIVE_FIELDS = {"cpf", "patient_id", "medical_record_number"}
