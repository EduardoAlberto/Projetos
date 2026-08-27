from pyspark.sql import SparkSession
import requests
from bs4 import BeautifulSoup
import os
import re
from data_engineer.demografia.pipeline_ingestao.src.extract.atendimentoParaImigrantes import extrair_links_IM, baixar
from data_engineer.demografia.pipeline_ingestao.src.extract.bolsaFamilia import extrair_links_BF, baixar  

# =========================
# 🔥 SPARK
# =========================
spark = SparkSession.builder \
    .appName("Download Paralelo CKAN") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext


# =========================
# 🚀 IMIGRANTES
# =========================
links = extrair_links_IM()

print(f"🔎 Encontrados {len(links)} links")

# 🔥 paraleliza com Spark
rdd = sc.parallelize(links, numSlices=4)

resultados = rdd.map(baixar).collect()

# print resultados
for r in resultados:
    print(r)


# =========================
# 🚀 BOLSA FAMILIA
# =========================
links = extrair_links_BF()

if not links:
    print("⚠️ Nenhum link encontrado")
else:
    print("🚀 Iniciando downloads paralelos...")

    rdd = sc.parallelize(links, numSlices=4)

    resultados = rdd.map(baixar).collect()

    for r in resultados:
        print(r)

print("\n🎯 Finalizado")

spark.stop()