import os
from pathlib import Path

class Config:
    # Configurações do Spark
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "Netflix")
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")

    # Paths configuráveis via variáveis de ambiente
    BASE_INPUT_PATH = os.getenv("NETFLIX_INPUT_PATH", "./data/input")
    BASE_OUTPUT_PATH = os.getenv("NETFLIX_OUTPUT_PATH", "./data/output")
    
    # .CSV Configurações
    CSV_FILE = os.getenv("NETFLIX_CSV_FILE", "netflix_titles_clean.csv")
    CSV_INPUT_PATH = os.path.join(BASE_INPUT_PATH, CSV_FILE)

    # Fontes de dados
    DATA_SOURCES = {
        "jdbc_url": os.getenv("JDBC_URL", "jdbc:postgresql://localhost:5432/netflix_db"),
        "db_user": os.getenv("DB_USER", "postgres"),
        "db_password": os.getenv("DB_PASSWORD", "postgre123")
    }

    # Armazenamento
    STORAGE = {
        "data_lake_path": os.path.join(BASE_OUTPUT_PATH, "netflix/processados")
    }
    
    @staticmethod
    def validate_input_path():
        """Valida se o arquivo CSV existe"""
        if not Path(Config.CSV_INPUT_PATH).exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {Config.CSV_INPUT_PATH}")
        return Config.CSV_INPUT_PATH
    
    @staticmethod
    def ensure_output_paths():
        """Garante que os diretórios de saída existem"""
        Path(Config.STORAGE["data_lake_path"]).mkdir(parents=True, exist_ok=True)

