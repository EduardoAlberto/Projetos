from .db import TABLE

TABLE_2025 = 'bronze.base_de_dados_crai_a_partir_de_2025'

SUMMARY = f"""
SELECT
    COUNT(*) AS total_registros,
    COUNT(*) FILTER (WHERE NULLIF(BTRIM(pais_origem), '') IS NULL) AS pais_nao_informado,
    COUNT(*) FILTER (WHERE NULLIF(BTRIM(escolaridade), '') IS NULL) AS escolaridade_nao_informada,
    COUNT(*) FILTER (WHERE demanda_1 = 'Regularização migratória') AS regularizacao_migratoria
FROM {TABLE}
"""

SUMMARY_2025 = f"""
SELECT
    COUNT(*) AS total_registros,
    COUNT(*) FILTER (
        WHERE NULLIF(BTRIM("País de Nascimento"), '') IS NULL
        OR LOWER(BTRIM("País de Nascimento")) IN ('nao informado', 'não informado', 'não informado.', 'n/a', 'n/a.', 'sem informação')
    ) AS pais_nao_informado,
    COUNT(*) FILTER (WHERE NULLIF(BTRIM("Pessoas__escolaridade"), '') IS NULL) AS escolaridade_nao_informada,
    COUNT(*) FILTER (
        WHERE LOWER(BTRIM("Tipo de Atendimento")) = 'regularização migratória'
        OR LOWER(BTRIM("Tipo de Atendimento")) = 'regularizacao migratoria'
    ) AS regularizacao_migratoria
FROM {TABLE_2025}
"""

DEMANDS_2025 = f"""
SELECT COALESCE(NULLIF(BTRIM("Tipo de Atendimento"), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE_2025}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

COUNTRIES_2025 = f"""
SELECT UPPER(NULLIF(BTRIM("País de Nascimento"), '')) AS categoria, COUNT(*) AS total
FROM {TABLE_2025}
GROUP BY 1
ORDER BY total DESC NULLS LAST
LIMIT 12
"""

AGES_2025 = f"""
SELECT COALESCE(NULLIF(BTRIM("Faixa Etária"), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE_2025}
GROUP BY 1
ORDER BY total DESC
"""

HOUSING_2025 = f"""
SELECT COALESCE(NULLIF(BTRIM("Pessoas__condicao_moradia"), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE_2025}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

MIGRATION_2025 = f"""
SELECT
    COALESCE(
        REGEXP_REPLACE(UPPER(NULLIF(BTRIM("Situação migratória"), '')), '\\s*/\\s*', ' / ', 'g'),
        'NÃO INFORMADO'
    ) AS categoria,
    COUNT(*) AS total
FROM {TABLE_2025}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

GENDER_2025 = f"""
SELECT COALESCE(NULLIF(BTRIM("Pessoas__identidade_genero"), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE_2025}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

BASE_2025_COUNT = f"""
SELECT COUNT(*) AS total_registros
FROM {TABLE_2025}
"""

BASE_2025_SAMPLE = f"""
SELECT dh_atendimento,
       "Equipamento",
       "Tipo de Atendimento",
       pessoas,
       "País de Nascimento",
       "Situação migratória",
       "Faixa Etária",
       "Distrito de Residência",
       "Pessoas__renda_familiar",
       "Pessoas__estado_civil",
       "Pessoas__religiao",
       "Pessoas__raca_cor",
       "Pessoas__escolaridade",
       "Pessoas__identidade_genero",
       "Pessoas__orientacao_sexual",
       "Pessoas__relacao_trabalho",
       "Pessoas__responsavel_familia",
       "Pessoas__condicao_moradia",
       "Pessoas__via_entrada"
FROM {TABLE_2025}
LIMIT 1000
"""

DEMANDS = f"""
SELECT COALESCE(NULLIF(BTRIM(demanda_1), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

COUNTRIES = f"""
SELECT UPPER(NULLIF(BTRIM(pais_origem), '')) AS categoria, COUNT(*) AS total
FROM {TABLE}
GROUP BY 1
ORDER BY total DESC NULLS LAST
LIMIT 12
"""

AGES = f"""
SELECT COALESCE(NULLIF(BTRIM(fx_etaria), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE}
GROUP BY 1
ORDER BY total DESC
"""

HOUSING = f"""
SELECT COALESCE(NULLIF(BTRIM(condicoes_moradia), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE}
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
FROM {TABLE}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""

GENDER = f"""
SELECT COALESCE(NULLIF(BTRIM(sexo), ''), 'Não informado') AS categoria, COUNT(*) AS total
FROM {TABLE}
GROUP BY 1
ORDER BY total DESC
LIMIT 10
"""
