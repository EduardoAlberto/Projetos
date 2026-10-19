# ======================================================
# Carga dos dataframes para a camada Silver
# ======================================================

from pyspark.sql.functions import col, regexp_replace, split, trim
from pyspark.sql.types import StringType

SILVER_SCHEMA = "silver"

CADUNICO_COLUMNS = [
    "COD_SEXO_PESSOA",
    "ANO_NASCIMENTO",
    "IDADE_EM_2020",
    "COD_RACA_COR_PESSOA",
    "COD_PARENTESCO_RF_PESSOA",
    "COD_LOCAL_NASCIMENTO_PESSOA",
    "NOM_PAIS_ORIGEM_PESSOA",
    "COD_DEFICIENCIA_MEMB",
    "IND_DEF_CEGUEIRA_MEMB",
    "IND_DEF_BAIXA_VISAO_MEMB",
    "IND_DEF_SURDEZ_PROFUNDA_MEMB",
    "IND_DEF_SURDEZ_LEVE_MEMB",
    "IND_DEF_FISICA_MEMB",
    "IND_DEF_MENTAL_MEMB",
    "IND_DEF_SINDROME_DOWN_MEMB",
    "IND_DEF_TRANSTORNO_MENTAL_MEMB",
    "IND_AJUDA_NAO_MEMB",
    "IND_AJUDA_FAMILIA_MEMB",
    "IND_AJUDA_ESPECIALIZADO_MEMB",
    "IND_AJUDA_VIZINHO_MEMB",
    "IND_AJUDA_INSTITUICAO_MEMB",
    "IND_AJUDA_OUTRA_MEMB",
    "COD_SABE_LER_ESCREVER_MEMB",
    "IND_FREQUENTA_ESCOLA_MEMB",
    "COD_CURSO_FREQUENTA_MEMB",
    "COD_CURSO_FREQUENTOU_PESSOA_MEMB",
    "COD_CONCLUIU_FREQUENTOU_MEMB",
    "COD_TRABALHOU_MEMB",
    "COD_AFASTADO_TRAB_MEMB",
    "COD_PRINCIPAL_TRAB_MEMB",
    "IND_VAL_REMUNER_EMPREGO_MEMB",
    "COD_TRABALHO_12_MESES_MEMB",
    "VAL_RENDA_BRUTA_12_MESES_MEMB",
    "VAL_RENDA_DOACAO_MEMB",
    "VAL_RENDA_APOSENT_MEMB",
    "VAL_RENDA_SEGURO_DESEMP_MEMB",
    "VAL_RENDA_PENSAO_ALIMEN_MEMB",
    "VAL_OUTRAS_RENDAS_MEMB",
    "DISTRITO",
    "SUBPREFEITURA",
    "SAS",
    "CRAS",
]


def _split_delimited_column(dataframe, source_column, output_columns):
    parts = split(col(source_column), ";", len(output_columns))
    original_columns = [column for column in dataframe.columns if column != source_column]
    return dataframe.withColumn("_split", parts).select(
        *[col(column) for column in original_columns],
        *[
            trim(col("_split").getItem(index)).alias(column_name)
            for index, column_name in enumerate(output_columns)
        ],
    )


def _normalize_dictionary(dataframe):
    expected = {"nome_campo", "descricao_campo"}
    if expected.issubset(dataframe.columns):
        return dataframe

    delimiter_columns = [column for column in dataframe.columns if ";" in column]
    source_column = delimiter_columns[0] if delimiter_columns else dataframe.columns[0]
    if len(dataframe.columns) == 1 or delimiter_columns:
        return _split_delimited_column(
            dataframe,
            source_column,
            ["nome_campo", "descricao_campo"],
        )

    return dataframe.select(
        *[col(column) for column in dataframe.columns[2:]],
        trim(col(dataframe.columns[0])).alias("nome_campo"),
        trim(col(dataframe.columns[1])).alias("descricao_campo"),
    )


def _transform_t2(dataframe):
    if set(CADUNICO_COLUMNS).issubset(dataframe.columns):
        return dataframe
    if len(dataframe.columns) != 1:
        raise ValueError("t2 deve conter uma coluna delimitada ou as 42 colunas esperadas")
    return _split_delimited_column(dataframe, dataframe.columns[0], CADUNICO_COLUMNS)


def _transform_t6(dataframe):
    dictionary = _normalize_dictionary(dataframe)
    return dictionary.filter(
        col("nome_campo").isNotNull()
        & col("descricao_campo").isNotNull()
        & (trim(col("nome_campo")) != "")
        & (trim(col("descricao_campo")) != "")
        & (~trim(col("nome_campo")).isin("VARIÁVEL", "VARIAVEL", "DICIONÁRIO DE VARIÁVEIS"))
    )


def transform_dataframes(dataframes):
    transforms = {
        "t1": _normalize_dictionary,
        "t2": _transform_t2,
        "t3": lambda dataframe: dataframe,
        "t4": _normalize_dictionary,
        "t5": _normalize_dictionary,
        "t6": _transform_t6,
        "t7": lambda dataframe: dataframe,
        "t8": lambda dataframe: dataframe,
    }
    transformed = dict(dataframes)
    for name, transform in transforms.items():
        if name not in transformed:
            raise ValueError(f"Dataframe obrigatório ausente: {name}")
        transformed[name] = transform(transformed[name])
    return transformed

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
    dataframes = transform_dataframes(dataframes)
    silver_tables = {
        "t1": (dataframes["t1"], "dicionario_crai_2025_t1"),
        "t2": (dataframes["t2"], "base_cadunico_imigrantes_2020_t2"),
        "t3": (dataframes["t3"], "base_crai_2014_2024_t3"),
        "t4": (dataframes["t4"], "dicionario_crai_2025_t4"),
        "t5": (dataframes["t5"], "dicionario_crai_2014_2019_t5"),
        "t6": (dataframes["t6"], "dicionario_cadunico_imigrantes_2020_t6"),
        "t7": (dataframes["t7"], "programa_bolsa_familia_2025_t7"),
        "t8": (dataframes["t8"], "base_crai_2025_t8"),
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