import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Configurações do Spark
    SPARK_MASTER = "local[*]"
    SPARK_APP_NAME = "Netflix"
    SPARK_EXECUTOR_MEMORY = "2g"

    # Fontes de dados
    DATA_SOURCES = {
        "jdbc_url": os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/netflix")
    }

    # Armazenamento
    STORAGE = {
        "data_lake_path": os.getenv("DATA_LAKE_PATH", "/Users/eduardoalberto/LoadFile/output/netflix/processados")
    }

