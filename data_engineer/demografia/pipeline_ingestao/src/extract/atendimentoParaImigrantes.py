from pyspark.sql import SparkSession
import requests
from bs4 import BeautifulSoup
import os
import re

# =========================
# 🔥 CONFIG
# =========================
URL = "https://dados.prefeitura.sp.gov.br/dataset/base-de-dados-do-centro-de-referencia-e-atendimento-para-imigrantes-crai"

OUTPUT_DIR = "/Users/eduardoalberto/LoadFile/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}

# =========================
# 🔥 UTIL
# =========================
def limpar_nome(nome):
    return re.sub(r'[\\/*?:"<>| ]', "_", nome)

# =========================
# 🔥 EXTRAIR LINKS
# =========================
def extrair_links_IM():
    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"]

        if "/download/" in href:
            if not href.startswith("http"):
                href = "https://dados.prefeitura.sp.gov.br" + href

            links.append(href)

    return list(set(links))  # remove duplicados

# =========================
# 🔥 DOWNLOAD PARALELO
# =========================
def baixar(link):
    try:
        nome = limpar_nome(link.split("/")[-1])

        if not nome or "." not in nome:
            nome = "arquivo.csv"

        caminho = os.path.join(OUTPUT_DIR, nome)

        if os.path.exists(caminho):
            return f"⏭️ Já existe: {nome}"

        response = requests.get(link, headers=HEADERS, stream=True, timeout=30)

        if response.status_code == 200:
            with open(caminho, "wb") as f:
                for chunk in response.iter_content(8192):
                    f.write(chunk)

            return f"✅ Download: {nome}"
        else:
            return f"❌ Erro {response.status_code} - {link}"

    except Exception as e:
        return f"❌ Falha {link} - {e}"

