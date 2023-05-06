#criando um objeto sparksession object e um appName 
from pyspark.sql import SparkSession
from Source import carga

spark=SparkSession.builder.master("local[1]")\
        .appName("Football")\
        .getOrCreate()

# carga(spark)