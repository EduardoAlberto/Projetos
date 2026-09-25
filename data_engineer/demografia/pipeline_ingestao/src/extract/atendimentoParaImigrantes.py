import hashlib
import os
import re
from pathlib import Path
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

# =========================
# 🔥 CONFIG
# =========================
URL = "https://dados.prefeitura.sp.gov.br/dataset/base-de-dados-do-centro-de-referencia-e-atendimento-para-imigrantes-crai"

OUTPUT_DIR = os.getenv("RAW_DIR", "/Users/eduardoalberto/LoadFile/raw")
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
    response = requests.get(URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
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
        nome = limpar_nome(Path(urlparse(link).path).name)

        if not nome or "." not in nome:
            sufixo = hashlib.sha256(link.encode()).hexdigest()[:16]
            nome = f"arquivo_{sufixo}.csv"

        caminho = os.path.join(OUTPUT_DIR, nome)

        if os.path.exists(caminho):
            return f"⏭️ Já existe: {nome}"

        response = requests.get(link, headers=HEADERS, stream=True, timeout=30)

        response.raise_for_status()
        temporario = f"{caminho}.part"
        with open(temporario, "wb") as arquivo:
            for chunk in response.iter_content(8192):
                if chunk:
                    arquivo.write(chunk)
        os.replace(temporario, caminho)
        return f"Download: {nome}"

    except Exception as e:
        return f"❌ Falha {link} - {e}"

