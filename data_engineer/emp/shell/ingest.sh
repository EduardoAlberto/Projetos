#!/bin/bash

# Variáveis
SPARK_HOME="/Users/eduardoalberto/opt/spark-3.5.4"
SCRIPT_PATH="/Users/eduardoalberto/Projetos/data_engineer/emp/src/config.py"
LOG_DIR="/Users/eduardoalberto/Projetos/data_engineer/emp/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/job_$TIMESTAMP.log"
ERROR_LOG="$LOG_DIR/error_$TIMESTAMP.log"
LOG_EXPIRATION_DAYS=1

# Criar diretório de log se não existir
mkdir -p "$LOG_DIR"

# Expurgo de logs antigos
echo "🧹 Limpando logs com mais de $LOG_EXPIRATION_DAYS dias..." | tee -a "$LOG_FILE"
find "$LOG_DIR" -type f -name "job_*.log" -mtime +$LOG_EXPIRATION_DAYS -exec rm -f {} \;
find "$LOG_DIR" -type f -name "error_*.log" -mtime +$LOG_EXPIRATION_DAYS -exec rm -f {} \;

echo "🚀 Iniciando Spark job..." | tee -a "$LOG_FILE"

# Submissão do Spark
$SPARK_HOME/bin/spark-submit \
    --master local[4] \
    --name emp \
    --packages io.delta:delta-spark_2.12:3.1.0,uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.5,org.apache.spark:spark-avro_2.12:3.5.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.sql.warehouse.dir=/Users/eduardoalberto/LoadFile/output/" \
    --driver-class-path /Users/eduardoalberto/opt/spark-3.5.4/jars/mysql-connector-j-9.1.0.jar \
    --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=file:/Users/eduardoalberto/opt/spark-3.5.4/conf/log4j2.properties" \
    "$SCRIPT_PATH" >> "$LOG_FILE" 2>> "$ERROR_LOG"

# Verificar status de execução
STATUS=$?

if [ $STATUS -eq 0 ]; then
    echo "✅ Job executado com sucesso às $(date '+%H:%M:%S')" | tee -a "$LOG_FILE"
else
    echo "❌ Erro na execução do Spark job (código $STATUS)" | tee -a "$ERROR_LOG"
    echo "🔍 Veja detalhes em: $ERROR_LOG"
fi
