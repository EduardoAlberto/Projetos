import os
from pathlib import Path
from raw import bulk_load_all

POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DATABASE", "data_lake"),
    "schema": os.getenv("POSTGRES_SCHEMA", "bronze"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
    "batchsize": 5000,
    "fetchsize": 1000,
    "numPartitions": 4,
}

CSV_DIRS = [
    Path(os.getenv("STAGING_DIR", "/Users/eduardoalberto/LoadFile/staging")) / "kmz",
    Path(os.getenv("STAGING_DIR", "/Users/eduardoalberto/LoadFile/staging")) / "csv",
    Path(os.getenv("STAGING_DIR", "/Users/eduardoalberto/LoadFile/staging")) / "ods",
]

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    if not POSTGRES["password"]:
        raise RuntimeError("Defina POSTGRES_PASSWORD antes de executar a ingestao")

    spark = (
        SparkSession.builder
        .appName("Bulk Load PostgreSQL")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.warehouse.dir", os.getenv("SPARK_WAREHOUSE_DIR", "/tmp/demografia-spark"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        results = bulk_load_all(spark, POSTGRES, CSV_DIRS)
        if any(not result["sucesso"] for result in results):
            raise RuntimeError("Uma ou mais tabelas Bronze falharam")
    finally:
        spark.stop()
