from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os 
import os.path as path
from  datetime import datetime 
import sys

#variavel de data / Diretorio dos arquivos
dt_ultima_data = datetime.now()
pfile = f"/Users/eduardoalberto/LoadFile/data_{dt_ultima_data}.csv"
ldir  =  "/Users/eduardoalberto/LoadFile/"

# print(dt_ultima_data)

if __name__ == "__main__":
      spark=SparkSession.builder.master("local[1]")\
      .appName("movie")\
      .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
      .getOrCreate()

l = (40 * "#")  
print( l + " \n" +  str(dt_ultima_data) +"\n" + l)










