import pyspark 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from  pyspark.sql.types import *
from os import path
import subprocess


def validaArquivo(self,spark):
    proc = subprocess.Popen(['hdfs','dfs','-test','-e',self])
    proc.communicate()

    if proc.returncode == 0:
        schema = (StructType()
                    .add("title",StringType(),True)
                    .add("handheld",StringType(),True)
                    .add("maxplayers",StringType(),True)
                    .add("multiplatform",StringType(),True)
                    .add("online",StringType(),True)
                    .add("genres",StringType(),True)
                    .add("licensed",StringType(),True)
                    .add("publishers",StringType(),True)
                    .add("sequel",StringType(),True)
                    .add("reviewscore",StringType(),True)
                    .add("sales",DoubleType(),True)
                    .add("usedprice",StringType(),True)
                    .add("console",StringType(),True)
                    .add("rating",StringType(),True)
                    .add("rerelease",StringType(),True)
                    .add("year",StringType(),True)
                    .add("playstylesaverage",DecimalType(8,2),True)
                    .add("playstylesleisure",DecimalType(8,2),True)
                    .add("playstylesmedian",DecimalType(8,2),True)
                    .add("playstylespolled",DecimalType(8,2),True)
                    .add("playstylesrushed",DecimalType(8,2),True)
                    .add("completionistsaverage",DecimalType(8,2),True)
                    .add("completionistsleisure",DecimalType(8,2),True)
                    .add("completionistsmedian",DecimalType(8,2),True)
                    .add("completionistspolled",DecimalType(8,2),True)
                    .add("completionistsrushed",DecimalType(8,2),True)
                    .add("extrasaverage",DecimalType(8,2),True)
                    .add("extrasleisure",DecimalType(8,2),True)
                    .add("extrasmedian",DecimalType(8,2),True)
                    .add("extraspolled",DecimalType(8,2),True)
                    .add("extrasrushed",DecimalType(8,2),True)
                    .add("storyaverage",DecimalType(8,2),True)
                    .add("storyleisure",DecimalType(8,2),True)
                    .add("storymedian",DecimalType(8,2),True)
                    .add("storypolled",DecimalType(8,2),True)
                    .add("storyrushed",DecimalType(8,2),True))
        arq = spark.read.csv(self,schema=schema, header=False)

    return arq

def carregaArquivo(self,spark):
    file = validaArquivo(self,spark)

    file = file.withColumn("dat_ref_carga",F.lit(F.current_date()))

    file.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.videogame",properties={"user": "root", "password": "mysql"})

    return None