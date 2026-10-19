from __future__ import annotations

import os
from collections.abc import Mapping, Sequence

import pandas as pd
from dotenv import load_dotenv
from psycopg_pool import ConnectionPool

load_dotenv()

TABLE = {
    'base_cadunico_imigrantes_2020_t2': 'silver.base_cadunico_imigrantes_2020_t2',
    'base_crai_2014_2024_t3': 'silver.base_crai_2014_2024_t3',
    'base_crai_2025_t8': 'silver.base_crai_2025_t8',
    'dicionario_cadunico_imigrantes_2020_t6': 'silver.dicionario_cadunico_imigrantes_2020_t6',
    'dicionario_crai_2014_2019_t5': 'silver.dicionario_crai_2014_2019_t5',
    'dicionario_crai_2025_t1': 'silver.dicionario_crai_2025_t1',
    'dicionario_crai_2025_t4': 'silver.dicionario_crai_2025_t4',
    'programa_bolsa_familia_2025_t7': 'silver.programa_bolsa_familia_2025_t7',
}


def _database_url() -> str:
    return (
        f"host={os.getenv('DB_HOST', 'localhost')} "
        f"port={os.getenv('DB_PORT', '5432')} "
        f"dbname={os.getenv('DB_NAME', 'data_lake')} "
        f"user={os.getenv('DB_USER', 'postgres')} "
        f"password={os.getenv('DB_PASSWORD', '')} "
        f"connect_timeout={os.getenv('DB_CONNECT_TIMEOUT', '10')}"
    )


def get_pool() -> ConnectionPool:
    return ConnectionPool(
        conninfo=_database_url(),
        min_size=1,
        max_size=5,
        open=True,
        name='dashboards-crai',
    )


def read_query(pool: ConnectionPool, query: str, params: Sequence | Mapping | None = None) -> pd.DataFrame:
    with pool.connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [column.name for column in cursor.description]
    return pd.DataFrame(rows, columns=columns)
