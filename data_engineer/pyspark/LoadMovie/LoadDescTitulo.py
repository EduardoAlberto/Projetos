from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os 
import os.path as path
import datetime as dt
import sys
#criando um objeto sparksession object e um appName 
spark=SparkSession.builder.master("local[1]")\
        .appName("movie")\
        .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
        .getOrCreate()

#variavel de data / Diretorio dos arquivos
dt_ultima_data = (dt.date.today() - dt.timedelta(9)).strftime('%Y%m%d')
pfile = f"/Users/eduardoalberto/LoadFile/data_{dt_ultima_data}.csv"
ldir  =  "/Users/eduardoalberto/LoadFile/"

if __name__ == "__main__":
        if path.exists(pfile):
            # arquivo file delimitado por ","
            df04 = spark.read.option("delimiter", "\t").option("header", True).csv(pfile)
            df04 = df04.select("tconst","titleType","primaryTitle","originalTitle","isAdult","startYear",
                                when(df04.endYear == r"\N", "1900").otherwise(df04.endYear).alias("endYear")
                                ,"runtimeMinutes","genres"
                                ,current_date().alias("dt_atualizacao")).filter(df04.isAdult > "0")
            # cria tabela no sql server
            df04.select("*").write.mode("overwrite").jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_descTitulo",properties={"user": "sa", "password": "Numsey@Password!"})
            # quantidade de itens inseridos
            total = spark.read.jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_descTitulo",properties={"user": "sa", "password": "Numsey@Password!"}).count()
            # print da variavel
            print(f"TABELA CRIANDO NO SQL SERVER, TOTAL DE REGISTRO:{total}")
        else:
            obj = os.scandir(ldir)
            print(f"DIRETORIO DOS ARQUIVOS:{ldir}")
            for dir in obj:
                if dir.is_dir() or dir.is_file():
                    print(dir.name)
            obj.close()


spark.stop()

