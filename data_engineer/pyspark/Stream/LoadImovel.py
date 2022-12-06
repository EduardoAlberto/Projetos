# %%
import pandas as pd
import datetime as dt
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
import os 
import os.path as path
import datetime as dt
import sys

spark=SparkSession.builder.master("local[1]")\
        .appName("Imoveis")\
        .getOrCreate()


sc = spark.sparkContext
json = spark.read.json("/Users/eduardoalberto/LoadFile/dataset_bruto.json")
spark.sparkContext.setLogLevel("OFF") 

def tratamentojson(df):
    schema =   StructType([
                StructField("andar",            LongType(),True),
                StructField("area_total",       ArrayType(StringType()),True),
                StructField("area_util",        ArrayType(StringType()),True),
                StructField("tipo_anuncio",     StringType(),True),
                StructField("tipo_unidade",     StringType(),True),
                StructField("andar02",          StringType(),True),
                StructField("vaga",             ArrayType(StringType()),True),
                StructField("banheiros",        ArrayType(StringType()),True),
                StructField("tipo_uso",         StringType(),True),
                StructField("caracteristicas",  ArrayType(StringType()),True),
                StructField("endereco",         ArrayType(StringType()),True),
                StructField("id",               StringType(),True),
                StructField("quartos",          ArrayType(StringType()),True),
                StructField("valores",          ArrayType(StringType()),True),
                StructField("suites",           ArrayType(StringType()),True),

        ]) 

    df = df.select(df.anuncio)
    df = df.withColumn("anuncio", to_json(f.col("anuncio")))
    df = df.withColumn("anuncio", from_json("anuncio", schema)).select(col('anuncio.*'))
    df = df.filter((df.tipo_uso == "Residencial") & (df.tipo_unidade == "Apartamento") & (df.tipo_anuncio == "Usado"))
    df = df.withColumn("quartos", regexp_replace(regexp_replace(df.quartos,r"\[",""),r"\]",""))
    df = df.withColumn("suites", regexp_replace(regexp_replace(df.suites,r"\[",""),r"\]",""))
    df = df.withColumn("banheiros", regexp_replace(regexp_replace(df.banheiros,r"\[",""),r"\]",""))
    df = df.withColumn("vaga", regexp_replace(regexp_replace(df.vaga,r"\[",""),r"\]",""))
    df = df.withColumn("area_total", regexp_replace(regexp_replace(df.area_total,r"\[",""),r"\]",""))
    df = df.withColumn("area_util", regexp_replace(regexp_replace(df.area_util,r"\[",""),r"\]",""))
    df2 = df.withColumn("valores", regexp_replace(regexp_replace(df.valores,r"\[",""),r"\]","")).select("id","valores")
    df2 = df2.select("id",json_tuple(col("valores"),"valor","tipo")).toDF("id2" ,"valor","tipo")
    df3 = df.join(df2, df.id == df2.id2, "inner")\
            .where(df2.tipo == "Venda")\
            .select( "andar"
                    ,"area_total"
                    ,"area_util"
                    ,"tipo_anuncio"
                    ,"tipo_unidade"
                    ,"vaga"
                    ,"banheiros"
                    ,"tipo_uso"
                    # ,"endereco"
                    ,"id"
                    ,"quartos"
                    ,"suites"
                    ,"valor"
                    ,"tipo" )


    df3.write.format("csv").mode("overwrite").options(header="true",sep="\t").save(path="/Users/eduardoalberto/LoadFile/CSV/")
    df3.write.parquet(path="/Users/eduardoalberto/LoadFile/parquet", mode="overwrite")    
    return df3
    
tratamentojson(json)




