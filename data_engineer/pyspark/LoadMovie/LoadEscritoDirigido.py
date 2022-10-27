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
# cria classe com Data type
schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors01", StringType(), True),
    StructField("directors02", StringType(), True),
    StructField("writers", StringType(), True),
    StructField("t_atualizacao", DateType(), True),
])

if __name__ == "__main__":
    #criando um objeto sparksession object e um appName 
    spark=SparkSession.builder.master("local[1]")\
          .appName("movie")\
          .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
          .getOrCreate()
    # verifica se arquivo existe
    if path.exists(pfile):
        # arquivo file delimitado por ","
        df03 = spark.read.option("delimiter", "\t").option("header", True).csv(pfile)
        # cria novos campos apartir de um substring
        df03 = df03.select("tconst",substring(df03.directors,1,9).alias("directors01"),substring(df03.directors,11,9).alias("directors02"),"writers")
        # substitui campos "nulos" por "0000000000"
        df03 = df03.select("tconst",
                    when(df03.directors01 == r"\N","000000000").otherwise(df03.directors01).alias("directors01"),
                    when(df03.directors02 == "","000000000").otherwise(df03.directors02).alias("directors02"),
                    when(df03.writers == r"\N","000000000").otherwise(df03.writers).alias("writers"),                    
                    current_date().alias("dt_atualizacao"))
        # cria um novo Dataframe com a classe criada
        df03 = spark.createDataFrame(df03.rdd, schema=schema)  
        #  cria tabela no sql server
        df03 = df03.select("*").limit(1000).write.mode("overwrite").jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_escrito_dirigido",properties={"user": "sa", "password": "Numsey@Password!"})
        # quantidade de itens inseridos
        total = spark.read.jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_escrito_dirigido",properties={"user": "sa", "password": "Numsey@Password!"}).count()        
        # print do total
        print(f"TABELA CRIANDO NO SQL SERVER, TOTAL DE REGISTRO:{total}")
    else:
        obj = os.scandir(ldir)
        print(f"DIRETORIO DOS ARQUIVOS:{ldir}")
        for dir in obj:
            if dir.is_dir() or dir.is_file():
                print(dir.name)
        obj.close()

