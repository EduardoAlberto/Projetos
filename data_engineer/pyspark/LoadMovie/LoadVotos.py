from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os 
import os.path as path
import datetime as dt
import sys

#variavel de data / Diretorio dos arquivos
dt_ultima_data = (dt.date.today() - dt.timedelta(2)).strftime('%Y%m%d')
pfile = f"/Users/eduardoalberto/LoadFile/data_{dt_ultima_data}.csv"
ldir  =  "/Users/eduardoalberto/LoadFile/"

# print(dt_ultima_data)

if __name__ == "__main__":
      spark=SparkSession.builder.master("local[1]")\
      .appName("movie")\
      .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
      .getOrCreate()
      
      # verifica se arquivo existe
      if path.exists(pfile):
            df01 = spark.read.option("delimiter", "\t").option("header", True).option("inferSchema", True).csv(pfile)
            df01 = df01.select("tconst","averageRating","numVotes",current_date().alias("dt_atualizacao"))
            df01.select("*").limit(1000).write.mode("overwrite").jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_votos",properties={"user": "sa", "password": "Numsey@Password!"})
            total = spark.read.jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_votos",properties={"user": "sa", "password": "Numsey@Password!"})
            total_registro = total.count()
            print(f"TABELA CRIANDO NO SQL SERVER, TOTAL DE REGISTRO:{total_registro}")
      else:
            obj = os.scandir(ldir)
            print(f"DIRETORIO DOS ARQUIVOS: {ldir}")
            for entry in obj :
                if entry.is_dir() or entry.is_file():
                    print(entry.name)
            obj.close()









