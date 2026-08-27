from pyspark.sql import SparkSession
import requests
from bs4 import BeautifulSoup
import os
import hashlib
import re

# =========================
# 🔥 CONFIG
# =========================
URL = "https://dados.prefeitura.sp.gov.br/dataset/numero-de-familas-beneficiarias-do-programa-bolsa-familia-por-distrito"

OUTPUT_DIR = "/Users/eduardoalberto/LoadFile/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

HEADERS = {"User-Agent": "Mozilla/5.0"}


# =========================
# 🧼 LIMPEZA
# =========================
def limpar_nome(nome):
    return re.sub(r'[\\/*?:"<>| ]', "_", nome)

# =========================
# 🔐 HASH
# =========================
def gerar_hash_bytes(conteudo):
    return hashlib.md5(conteudo).hexdigest()

def arquivo_existe_e_igual(caminho, conteudo):
    if not os.path.exists(caminho):
        return False

    try:
        with open(caminho, "rb") as f:
            antigo = f.read()

        return gerar_hash_bytes(antigo) == gerar_hash_bytes(conteudo)
    except:
        return False

# =========================
# 🔍 EXTRAIR LINKS (CKAN)
# =========================
def extrair_links_BF():
    print("🌐 Acessando dataset...")

    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    links = set()

    for a in soup.select("a.resource-url-analytics"):
        href = a.get("href")

        if href:
            if not href.startswith("http"):
                href = "https://dados.prefeitura.sp.gov.br" + href

            links.add(href)

    print(f"🔎 {len(links)} links encontrados")

    return list(links)

# =========================
# 🚀 DOWNLOAD DISTRIBUÍDO
# =========================
def baixar(link):
    try:
        nome = limpar_nome(link.split("/")[-1])

        if not nome or "." not in nome:
            nome = f"arquivo_{hashlib.md5(link.encode()).hexdigest()}.csv"

        caminho = os.path.join(OUTPUT_DIR, nome)

        response = requests.get(
            link,
            headers=HEADERS,
            stream=True,
            allow_redirects=True,
            timeout=60
        )

        if response.status_code != 200:
            return f"❌ {response.status_code} - {link}"

        content_type = response.headers.get("Content-Type", "").lower()

        if "html" in content_type:
            return f"⚠️ Ignorado HTML: {link}"

        conteudo = response.content

        if arquivo_existe_e_igual(caminho, conteudo):
            return f"⏭️ Já existe: {nome}"

        with open(caminho, "wb") as f:
            f.write(conteudo)

        return f"✅ Baixado: {nome}"

    except Exception as e:
        return f"❌ Erro: {e}"

