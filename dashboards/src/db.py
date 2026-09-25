from __future__ import annotations

import os
from collections.abc import Mapping, Sequence

import pandas as pd
from dotenv import load_dotenv
from psycopg_pool import ConnectionPool

load_dotenv()

TABLE = 'bronze.bancocrai2014a2024_sistematizacao_geoinfo_atualizada'


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
