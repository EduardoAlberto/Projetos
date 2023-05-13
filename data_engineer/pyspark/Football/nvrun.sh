# application spark standlone clusterß 
spark-submit \
 --executor-memory 5G \
 --executor-cores 8 \
 --master spark://207.184.161.138:7077 \
    /Users/eduardoalberto/Projetos/data_engineer/pyspark/Football/SourceLoad.py \