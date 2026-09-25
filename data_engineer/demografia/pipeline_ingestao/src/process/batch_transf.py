# ======================================================
# Carga dos dataframes para a camada Silver
# ======================================================

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StringType

SILVER_SCHEMA = "silver"

def clean_for_silver(dataframe):
    """Remove bytes nulos e padroniza os nomes das colunas para a camada Silver."""
    dataframe = dataframe.select(
        *[
            regexp_replace(col(column_name), "\\u0000", "").alias(column_name)
            if isinstance(dataframe.schema[column_name].dataType, StringType)
            else col(column_name)
            for column_name in dataframe.columns
        ]
    )

    return dataframe


def load_to_silver(dataframe, table_name, jdbc_url, postgres_config):
    """Grava um dataframe na tabela Silver correspondente."""
    qualified_table_name = f"{SILVER_SCHEMA}.{table_name}"
    print(f"Carregando {qualified_table_name}...")

    (
        clean_for_silver(dataframe)
        .write
        .format("jdbc")
        .mode("overwrite")
        .option("url", jdbc_url)
        .option("dbtable", qualified_table_name)
        .option("user", postgres_config["user"])
        .option("password", postgres_config["password"])
        .option("driver", postgres_config["driver"])
        .option("batchsize", postgres_config["batchsize"])
        .option("numPartitions", postgres_config["numPartitions"])
        .save()
    )

    print(f"Concluído: {qualified_table_name}")


def load_dataframes_to_silver(dataframes, jdbc_url, postgres_config):
    silver_tables = {
        "t1": (dataframes["t1"], "dicionario_crai_2025_t1"),
        "t2": (dataframes["t2"], "base_cadunico_imigrantes_2020_t2"),
        "t3": (dataframes["t3"], "base_crai_2014_2024_t3"),
        "t4": (dataframes["t4"], "dicionario_crai_2025_t4"),
        "t5": (dataframes["t5"], "dicionario_crai_2014_2019_t5"),
        "t6": (dataframes["t6"], "dicionario_cadunico_imigrantes_2020_t6"),
        "t7": (dataframes["t7"], "programa_bolsa_familia_2025_t7"),
    }

    silver_results = []

    for dataframe_name, (dataframe, table_name) in silver_tables.items():
        try:
            load_to_silver(dataframe, table_name, jdbc_url, postgres_config)
            silver_results.append({
                "dataframe": dataframe_name,
                "tabela": f"{SILVER_SCHEMA}.{table_name}",
                "sucesso": True,
            })
        except Exception as error:
            print(f"Erro ao carregar {dataframe_name}: {error}")
            silver_results.append({
                "dataframe": dataframe_name,
                "tabela": f"{SILVER_SCHEMA}.{table_name}",
                "sucesso": False,
                "erro": str(error),
            })

    print("\nResumo da carga Silver:")
    for result in silver_results:
        status = "Sucesso" if result["sucesso"] else "Erro"
        print(f"{status}: {result['dataframe']} -> {result['tabela']}")

    return silver_results