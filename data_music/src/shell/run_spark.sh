#!/bin/bash

# Configurações do ambiente PySpark
export SPARK_HOME=/Users/eduardoalberto/opt/Spark
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Caminho para o script PySpark
SCRIPT_PATH=/Users/eduardoalberto/Projetos/data_music/src/pyspark/run_etl.py

# Comando para executar o script PySpark
$SPARK_HOME/bin/spark-submit \
  --master local[2] \
  --name MySparkApp \
  $SCRIPT_PATH