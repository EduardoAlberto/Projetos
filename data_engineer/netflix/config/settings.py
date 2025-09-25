import os

class Config:
    # Configurações do Spark
    SPARK_MASTER = "local[*]"
    SPARK_APP_NAME = "Netflix"
    SPARK_EXECUTOR_MEMORY = "2g"

    # .CSV Configurações
    CSV_OPTIONS = {
        "arq": os.getenv("data_arq", "/Users/eduardoalberto/LoadFile/input/netflix_titles_clean.csv")
    }

    # Fontes de dados
    DATA_SOURCES = {
        "jdbc_url": os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/dbpostgres")
    }

    # Armazenamento
    STORAGE = {
        "data_lake_path": os.getenv("DATA_LAKE_PATH", "/Users/eduardoalberto/LoadFile/output/netflix/processados")
    }

