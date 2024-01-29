#Config dos projetos
 Versao python 3.10 https://www.python.org/downloads/macos/
 mkdir projetos_kedro && cd projetos_kedro

#Criando um ambiente virtual
 python3.10 -m venv .venv
 python3.9 -m venv .venv

acessando envsource venv/bin/activate 

#Ativando python 
 
 pip install pipenv
 pipenv shell

#run spark
docker exec -it spark-master /bin/bash

Docker exec -it spark-master spark-submit\
-- master spark://spark-master:7077 \
-- packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
jobs/spark-streaming.py

#atualizacao
src git:(main) ✗ docker compose up -d --build  

