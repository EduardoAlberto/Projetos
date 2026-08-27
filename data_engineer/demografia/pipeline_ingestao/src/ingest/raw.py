# ======================================================
# Bulk Load CSV para PostgreSQL
# ======================================================
import re

from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StringType


def sanitize_table_name(file_name):
    """Converte o nome do arquivo em um nome de tabela válido no PostgreSQL."""
    table_name = re.sub(r"[^a-zA-Z0-9_]+", "_", file_name).strip("_").lower()
    return table_name[:63]


def clean_null_bytes(dataframe):
    """Remove o byte nulo, que não é aceito pelo PostgreSQL em texto UTF-8."""
    string_columns = {
        field.name
        for field in dataframe.schema.fields
        if isinstance(field.dataType, StringType)
    }

    return dataframe.select(
        *[
            regexp_replace(col(column_name), "\\u0000", "").alias(column_name)
            if column_name in string_columns
            else col(column_name)
            for column_name in dataframe.columns
        ]
    )


def bulk_load_csv(spark, csv_path, postgres_config):
    table_name = sanitize_table_name(csv_path.stem)
    qualified_table_name = f"{postgres_config['schema']}.{table_name}"
    print(f"Processando {csv_path.name} -> {qualified_table_name}")

    dataframe = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("encoding", "UTF-8")
        .option("escape", '"')
        .csv(str(csv_path))
    )
    dataframe = clean_null_bytes(dataframe)

    jdbc_url = (
        f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}"
        f"/{postgres_config['database']}"
    )

    (
        dataframe.write
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

    return {"arquivo": csv_path.name, "tabela": qualified_table_name, "sucesso": True}


def bulk_load_all(spark, postgres_config, csv_dirs):
    """Carrega os CSVs das pastas configuradas no PostgreSQL."""
    csv_files = sorted(
        csv_file
        for csv_dir in csv_dirs
        for csv_file in csv_dir.glob("*.csv")
    )
    results = []

    for csv_file in csv_files:
        try:
            results.append(bulk_load_csv(spark, csv_file, postgres_config))
        except Exception as error:
            table_name = sanitize_table_name(csv_file.stem)
            qualified_table_name = f"{postgres_config['schema']}.{table_name}"
            print(f"Erro ao processar {csv_file.name}: {error}")
            results.append({
                "arquivo": csv_file.name,
                "tabela": qualified_table_name,
                "sucesso": False,
            })

    print("\nResumo do bulk load:")
    for result in results:
        status = "Sucesso" if result["sucesso"] else "Erro"
        print(f"{status}: {result['arquivo']} -> {result['tabela']}")

    sucessos = sum(result["sucesso"] for result in results)
    print(f"Total: {sucessos}/{len(results)} arquivo(s) carregado(s) com sucesso")
    return results


if __name__ == "__main__":
    from config import CSV_DIRS, POSTGRES, spark

    bulk_load_all(spark, POSTGRES, CSV_DIRS)
    spark.stop()