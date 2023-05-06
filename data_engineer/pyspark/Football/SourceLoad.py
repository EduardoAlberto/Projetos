import json, requests
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import *
import os.path as path

spark=SparkSession.builder.master("local[1]")\
    .appName("Music")\
    .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
    .getOrCreate()
spark.sparkContext.setLogLevel("OFF")
                   
# arquivo 
pfile = '/Users/eduardoalberto/LoadFile/BR-Football-Dataset.csv'
nfile = '/Users/eduardoalberto/LoadFile/BR-Football-DatasetOld.csv'

# valida se arquivo exists
def validaArq(df):
    if path.isfile(pfile):
        df = (spark.read
            .format("csv")
            .option("delimiter", ",")
            .option("header", True)
            .option("inferSchema", True)
            .load(pfile))
    else:
        df = (spark.read
            .format("csv")
            .option("delimiter", ",")
            .option("header", True)
            .option("inferSchema", True)
            .load(nfile))
    return df

df = validaArq(pfile)
df.show()
# df.write.mode("overwrite").jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_camp_brasileiro",properties={"user": "sa", "password": "Numsey@Password!"})




