import os
import shutil
import tempfile
import zipfile
import pandas as pd
import xml.etree.ElementTree as ET

RAW_DIR = "/Users/eduardoalberto/LoadFile/raw"
STAGING_DIR = "/Users/eduardoalberto/LoadFile/staging/"

CSV_STAGE = os.path.join(STAGING_DIR, "csv")
ODS_STAGE = os.path.join(STAGING_DIR, "ods")
KMZ_STAGE = os.path.join(STAGING_DIR, "kmz")

os.makedirs(CSV_STAGE, exist_ok=True)
os.makedirs(ODS_STAGE, exist_ok=True)
os.makedirs(KMZ_STAGE, exist_ok=True)


def process_csv(file_path):
    destino = os.path.join(CSV_STAGE, os.path.basename(file_path))
    shutil.copy(file_path, destino)
    print(f"CSV copiado -> {destino}")


def process_ods(file_path):
    df = pd.read_excel(file_path, engine="odf")
    nome = os.path.splitext(os.path.basename(file_path))[0]
    destino = os.path.join(CSV_STAGE, nome + ".csv")
    df.to_csv(destino, index=False, encoding="utf-8")
    print(f"ODS convertido -> {destino}")


def process_kmz(file_path):
    nome = os.path.splitext(os.path.basename(file_path))[0]

    with tempfile.TemporaryDirectory(prefix="kmz_", dir=KMZ_STAGE) as pasta_temp:
        with zipfile.ZipFile(file_path, "r") as kmz:
            kmz.extractall(pasta_temp)

        registros = []
        ns = {"kml": "http://www.opengis.net/kml/2.2"}

        for arquivo in os.listdir(pasta_temp):
            if arquivo.lower().endswith(".kml"):
                caminho = os.path.join(pasta_temp, arquivo)
                tree = ET.parse(caminho)
                root = tree.getroot()

                for placemark in root.findall(".//kml:Placemark", ns):
                    nome_ponto = placemark.findtext("kml:name", default="", namespaces=ns)
                    descricao = placemark.findtext("kml:description", default="", namespaces=ns)
                    coordenadas = placemark.findtext(".//kml:coordinates", default="", namespaces=ns)

                    registros.append(
                        {
                            "name": nome_ponto,
                            "description": descricao,
                            "coordinates": coordenadas,
                        }
                    )

                break

    if registros:
        df = pd.DataFrame(registros)
    else:
        df = pd.DataFrame({"conteudo": [f"Arquivo KMZ processado: {file_path}"]})

    destino = os.path.join(KMZ_STAGE, nome + ".csv")
    df.to_csv(destino, index=False, encoding="utf-8")
    print(f"KMZ convertido -> {destino}")


def process_generic(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    nome = os.path.splitext(os.path.basename(file_path))[0]
    destino = os.path.join(CSV_STAGE, nome + ".csv")

    try:
        if ext == ".ods":
            process_ods(file_path)
            return

        if ext == ".kmz":
            process_kmz(file_path)
            return

        if ext in {".xlsx", ".xls", ".xlsm"}:
            df = pd.read_excel(file_path)
        elif ext == ".json":
            df = pd.read_json(file_path)
        elif ext == ".parquet":
            df = pd.read_parquet(file_path)
        elif ext == ".feather":
            df = pd.read_feather(file_path)
        else:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as handle:
                text = handle.read()
            df = pd.DataFrame({"conteudo": [text]})

        df.to_csv(destino, index=False, encoding="utf-8")
        print(f"Arquivo convertido -> {destino}")
    except Exception as exc:
        fallback = pd.DataFrame({"arquivo": [os.path.basename(file_path)], "erro": [str(exc)]})
        fallback.to_csv(destino, index=False, encoding="utf-8")
        print(f"Erro na conversão, CSV de fallback criado -> {destino}")
        print(exc)


for arquivo in os.listdir(RAW_DIR):
    caminho = os.path.join(RAW_DIR, arquivo)
    ext = os.path.splitext(arquivo)[1].lower()

    try:
        if ext == ".csv":
            process_csv(caminho)
        else:
            process_generic(caminho)
    except Exception as e:
        print(f"Erro em {arquivo}")
        print(e)

print("Staging concluída.")