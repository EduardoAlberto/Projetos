import os
from concurrent.futures import ThreadPoolExecutor

from atendimentoParaImigrantes import baixar as baixar_imigrante
from atendimentoParaImigrantes import extrair_links_IM
from bolsaFamilia import baixar as baixar_bolsa
from bolsaFamilia import extrair_links_BF


def baixar_links(links, downloader):
    workers = int(os.getenv("DOWNLOAD_WORKERS", "4"))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(downloader, links))


def main():
    for resultado in baixar_links(extrair_links_IM(), baixar_imigrante):
        print(resultado)

    for resultado in baixar_links(extrair_links_BF(), baixar_bolsa):
        print(resultado)


if __name__ == "__main__":
    main()