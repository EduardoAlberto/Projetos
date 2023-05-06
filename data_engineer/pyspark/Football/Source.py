import json, requests
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import *

def carga(spark):

    uri = 'https://api.football-data.org/v4/competitions'
    headers = { 'X-Auth-Token': '3a48f133afd545dea4f0f25b5f42f400' }

    response = requests.get(uri, headers=headers)
    rdd = spark.sparkContext.parallelize([response.text])

    df = spark.read.option("multiline", "true").json(rdd)
    df1 = df.select(explode(df.competitions.area).alias("area"))
    df1 = df1.select(to_json(df1.area).alias("area"))
      

    schema = StructType(
      [
        StructField('code', StringType(), True),
        StructField('flag', StringType(), True),
        StructField('id', StringType(), True),
        StructField('name', StringType(), True)
      ]
    )

    df1 = df1.withColumn("area", from_json("area", schema))
    df1 = df1.select(col("area.*"))


    # currentSeason
    df2 = df.select(explode(df.competitions.currentSeason).alias("currentSeason"))
    df2 = df2.select(to_json(df2.currentSeason).alias("currentSeason"))

    schm02 = StructType(
      [
        StructField("currentMatchday", StringType(), True),
        StructField("endDate", StringType(), True),
        StructField("id", StringType(), True),
        StructField("startDate", StringType(), True),
        StructField("winner", StringType(), True)

      ]  

    )

    df2 = df2.withColumn("currentSeason", from_json("currentSeason", schm02))
    df2 = df2.select(col("currentSeason.*"))

    # competitions
    df3 = df.select(explode(df.competitions).alias("competitions"))
    df3 = df3.select(df3.competitions.emblem, \
                    df3.competitions.id, \
                    df3.competitions.lastUpdated, \
                    df3.competitions.numberOfAvailableSeasons, \
                    df3.competitions.plan, \
                    df3.competitions.type)

    df4 = df.select(col("competitions.name").alias("nome"))
    df4 = df4.withColumn("Liga", explode("nome")).select("Liga")

    # carga no banco

    df1.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/mydesenv", "tb_area",properties={"user": "root", "password": "mysql"})

    df2.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/mydesenv", "tb_currentSeason",properties={"user": "root", "password": "mysql"})

    df3.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/mydesenv", "tb_competitions",properties={"user": "root", "password": "mysql"})

    df4.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/mydesenv", "tb_liga",properties={"user": "root", "password": "mysql"})

    return 

