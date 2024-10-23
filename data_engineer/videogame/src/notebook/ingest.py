import pyspark.sql.functions as F
from pyspark.sql.types import *
from os import path


def validaArquivo(self,spark):
    if path.exists(self):
        schema = (StructType()
                 .add("Title",StringType(),True)
                 .add("Handheld",StringType(),True)
                 .add("Max Players",StringType(),True)
                 .add("Multiplatform",StringType(),True)
                 .add("Online",StringType(),True)
                 .add("Genres",StringType(),True)
                 .add("Licensed",StringType(),True)
                 .add("Publishers",StringType(),True)
                 .add("Sequel",StringType(),True)
                 .add("ReviewScore",StringType(),True)
                 .add("Sales",DoubleType(),True)
                 .add("UsedPrice",StringType(),True)
                 .add("Console",StringType(),True)
                 .add("Rating",StringType(),True)
                 .add("Rerelease",StringType(),True)
                 .add("Year",StringType(),True)
                 .add("PlayStylesAverage",DecimalType(8,2),True)
                 .add("PlayStylesLeisure",DecimalType(8,2),True)
                 .add("PlayStylesMedian",DecimalType(8,2),True)
                 .add("PlayStylesPolled",DecimalType(8,2),True)
                 .add("PlayStylesRushed",DecimalType(8,2),True)
                 .add("CompletionistsAverage",DecimalType(8,2),True)
                 .add("CompletionistsLeisure",DecimalType(8,2),True)
                 .add("CompletionistsMedian",DecimalType(8,2),True)
                 .add("CompletionistsPolled",DecimalType(8,2),True)
                 .add("CompletionistsRushed",DecimalType(8,2),True)
                 .add("ExtrasAverage",DecimalType(8,2),True)
                 .add("ExtrasLeisure",DecimalType(8,2),True)
                 .add("ExtrasMedian",DecimalType(8,2),True)
                 .add("ExtrasPolled",DecimalType(8,2),True)
                 .add("ExtrasRushed",DecimalType(8,2),True)
                 .add("StoryAverage",DecimalType(8,2),True)
                 .add("StoryLeisure",DecimalType(8,2),True)
                 .add("StoryMedian",DecimalType(8,2),True)
                 .add("StoryPolled",DecimalType(8,2),True)
                 .add("StoryRushed",DecimalType(8,2),True))
        arq = spark.read.csv(self,schema=schema, header=False)

    return arq

def carregaArquivo(self,spark):
    file = validaArquivo(self,spark)

    file = file.withColumn("dat_ref_carga",F.lit(F.current_date()))

    file.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.videogame",properties={"user": "root", "password": "mysql"})

    return None