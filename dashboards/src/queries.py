from .db import TABLE

SILVER_QUALITY = f"""
SELECT campo,
       preenchidos,
       total_registros - preenchidos AS ausentes,
       ROUND(100.0 * preenchidos / NULLIF(total_registros, 0), 1) AS percentual_preenchido
FROM (
    SELECT COUNT(*) AS total_registros,
           COUNT(NULLIF(BTRIM(pais_origem), '')) AS pais_origem,
           COUNT(NULLIF(BTRIM(escolaridade), '')) AS escolaridade,
           COUNT(NULLIF(BTRIM(demanda_1), '')) AS demanda_1,
           COUNT(NULLIF(BTRIM(fx_etaria), '')) AS fx_etaria,
           COUNT(NULLIF(BTRIM(condicoes_moradia), '')) AS condicoes_moradia,
           COUNT(NULLIF(BTRIM(situacao_migratoria), '')) AS situacao_migratoria,
           COUNT(NULLIF(BTRIM(sexo), '')) AS sexo
    FROM {TABLE['base_crai_2014_2024_t3']}
) AS contagens
CROSS JOIN LATERAL (
    VALUES
        ('Pais de origem', pais_origem),
        ('Escolaridade', escolaridade),
        ('Demanda principal', demanda_1),
        ('Faixa etaria', fx_etaria),
        ('Condicao de moradia', condicoes_moradia),
        ('Situacao migratoria', situacao_migratoria),
        ('Sexo', sexo)
) AS campos(campo, preenchidos)
ORDER BY percentual_preenchido ASC
"""

SILVER_COUNT = f"SELECT COUNT(*) AS total_registros FROM {TABLE['base_crai_2014_2024_t3']}"

SILVER_CATALOG = """
SELECT table_name,
       STRING_AGG(column_name, ', ' ORDER BY ordinal_position) AS columns
FROM information_schema.columns
WHERE table_schema = 'silver'
GROUP BY table_name
ORDER BY table_name
"""

SUMMARY = f"""
SELECT
    COUNT(*) AS total_registros,
    COUNT(*) FILTER (WHERE NULLIF(BTRIM(pais_origem), '') IS NULL) AS pais_nao_informado,
    COUNT(*) FILTER (WHERE NULLIF(BTRIM(escolaridade), '') IS NULL) AS escolaridade_nao_informada,
    COUNT(*) FILTER (WHERE demanda_1 = 'Regularização migratória') AS regularizacao_migratoria
FROM {TABLE['base_crai_2014_2024_t3']}
"""

DEMANDS = f"""
SELECT COALESCE(NULLIF(BTRIM(demanda_1), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE['base_crai_2014_2024_t3']}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

COUNTRIES = f"""
SELECT UPPER(NULLIF(BTRIM(pais_origem), '')) AS categoria, COUNT(*) AS total
FROM {TABLE['base_crai_2014_2024_t3']}
GROUP BY 1
ORDER BY total DESC NULLS LAST
LIMIT 12
"""

AGES = f"""
SELECT COALESCE(NULLIF(BTRIM(fx_etaria), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE['base_crai_2014_2024_t3']}
GROUP BY 1
ORDER BY total DESC
"""

HOUSING = f"""
SELECT COALESCE(NULLIF(BTRIM(condicoes_moradia), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE['base_crai_2014_2024_t3']}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

MIGRATION = f"""
SELECT
    COALESCE(
        REGEXP_REPLACE(UPPER(NULLIF(BTRIM(situacao_migratoria), '')), '\\s*/\\s*', ' / ', 'g'),
        'NÃO INFORMADO'
    ) AS categoria,
    COUNT(*) AS total
FROM {TABLE['base_crai_2014_2024_t3']}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

GENDER = f"""
SELECT COALESCE(NULLIF(BTRIM(sexo), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE['base_crai_2014_2024_t3']}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""
