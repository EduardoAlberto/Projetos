import os
from pyspark.sql import SparkSession
from batch_transf import load_dataframes_to_silver

CONF = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DATABASE", "data_lake"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
    "batchsize": 5000,
    "numPartitions": 4,
}

QUERIES = {
    "t1": "SELECT * FROM bronze.dicionario_base_de_dados_crai_a_partir_de_2025",
    "t2": "SELECT * FROM bronze.basecadunicoimigrantesmsp082020",
    "t3": "SELECT * FROM bronze.bancocrai2014a2024_sistematizacao_geoinfo_atualizada",
    "t5": "SELECT * FROM bronze.dicionario_de_bancocrai2014a2019",
    "t6": "SELECT * FROM bronze.dicionariobasecadunicoimigrantesmsp082020_1",
    "t7": "SELECT * FROM bronze.programabolsafamilia_202501",
}


def main():
    if not CONF["password"]:
        raise RuntimeError("Defina POSTGRES_PASSWORD antes de executar o processamento")

    spark = (
        SparkSession.builder
        .appName("Demografia Silver")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.warehouse.dir", os.getenv("SPARK_WAREHOUSE_DIR", "/tmp/demografia-spark"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    jdbc_url = f"jdbc:postgresql://{CONF['host']}:{CONF['port']}/{CONF['database']}"

    try:
        dataframes = {
            name: (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", CONF["user"])
                .option("password", CONF["password"])
                .option("driver", CONF["driver"])
                .load()
            )
            for name, query in QUERIES.items()
        }
        dataframes["t4"] = dataframes["t1"]
        results = load_dataframes_to_silver(dataframes, jdbc_url, CONF)
        if any(not result["sucesso"] for result in results):
            raise RuntimeError("Uma ou mais tabelas Silver falharam")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()