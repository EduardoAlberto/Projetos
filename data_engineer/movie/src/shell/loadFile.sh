# Configurações do ambiente PySpark
export SPARK_HOME=/Users/eduardoalberto/opt/spark-3.5.4
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH

# Caminho para o script PySpark
SCRIPT_PATH=/Users/eduardoalberto/Projetos/data_engineer/movie/src/notebook/config.py

# Comando para executar o script PySpark
$SPARK_HOME/bin/spark-submit \
  --master local \
  --name MySparkApp \
  $SCRIPT_PATH

echo carga finalida com sucesso