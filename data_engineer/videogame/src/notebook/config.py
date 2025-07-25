from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from ingest import validaArquivo, carregaArquivo


spark=SparkSession.builder.master("yarn")\
        .appName("VideoGame")\
        .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark-3.5.3/jars/mysql-connector-j-8.2.0.jar" ) \
        .getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("OFF") 
spark

file = '/Users/eduardoalberto/LoadFile/input/video_games.csv'
df = validaArquivo(file,spark)
print("TOTAL DE REGISTROS CARREGADOS: " + str(df.count()))
carregaArquivo(file,spark)