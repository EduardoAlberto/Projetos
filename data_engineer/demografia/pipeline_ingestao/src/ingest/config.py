import os
from pathlib import Path
from pyspark.sql import SparkSession
from raw import bulk_load_all

spark = (
    SparkSession.builder
    .appName("Bulk Load PostgreSQL")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/Users/eduardoalberto/LoadFile/output")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DATABASE", "dbpostgres"),
    "schema": os.getenv("POSTGRES_SCHEMA", "bronze"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgre123"),
    "driver": "org.postgresql.Driver",
    "batchsize": 5000,
    "fetchsize": 1000,
    "numPartitions": 4,
}

CSV_DIRS = [
    Path("/Users/eduardoalberto/LoadFile/staging/csv"),
    Path("/Users/eduardoalberto/LoadFile/staging/kmz"),
    Path("/Users/eduardoalberto/LoadFile/staging/ods"),
]

if __name__ == "__main__":
    bulk_load_all(spark, POSTGRES, CSV_DIRS)
    spark.stop()
