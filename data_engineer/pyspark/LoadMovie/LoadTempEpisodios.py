from asyncore import read
import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os.path as path
import sys
import argparse 

# parametro para execução do arquivo
parser = argparse.ArgumentParser()
pdata =  sys.argv[1]
pfile = f"/Users/eduardoalberto/LoadFile/data_{pdata}.csv"
ldir  = "/Users/eduardoalberto/LoadFile/"

#criando um objeto sparksession object e um appName 
if __name__ == "__main__":

    spark=SparkSession.builder.master("local[1]")\
          .appName("movie")\
          .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
          .getOrCreate()

      # verifica se arquivo existe
    if path.exists(pfile):
        df02 = spark.read.option("delimiter", "\t").option("header", True).csv(pfile)
        df02 = df02.select("tconst","parentTconst","seasonNumber","episodeNumber",current_date().alias("dt_atualizacao"))
        df02 = df02.select("*").limit(1000).write.mode("overwrite").jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_tempEpisodios",properties={"user": "sa", "password": "Numsey@Password!"})
        total = spark.read.jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_atores",properties={"user": "sa", "password": "Numsey@Password!"})
        total_registro = total.count()
        print(f"TABELA CRIANDO NO SQL SERVER, TOTAL DE REGISTRO:{total_registro}")
    else:
        obj = os.scandir(ldir)
        print(f"DIRETORIO DOS ARQUIVOS:{ldir}")
        for dir in obj:
            if dir.is_dir() or dir.is_file():
                print(dir.name)
        obj.close()





