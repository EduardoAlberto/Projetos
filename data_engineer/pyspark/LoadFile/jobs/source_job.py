import os 
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os.path as path


spark=SparkSession.builder.master("local[1]")\
    .appName("Music")\
    .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
    .getOrCreate()
spark.sparkContext.setLogLevel("OFF")

def arquivo(df):
    if path.exists(pfile):
        df = (spark.read
            .format("csv")
            .option("delimiter", ",")
            .option("header", True)
            .option("inferSchema", True)
            .load(pfile))
    return df
    
if __name__ == "__main__":
    pfile = "/Users/eduardoalberto/LoadFile/archive/genres_v2.csv"
    df = arquivo(pfile)
    df = df.select(df.title, 
                   df.genre,
                   df.song_name, 
                   df.duration_ms,
                   df.time_signature,
                   format_number(df.tempo,2).alias("tempo"), 
                   format_number(df.acousticness,2).alias("acousticness"), 
                   format_number(df.instrumentalness,2).alias("instrumentalness"))
    
    df.write.parquet(path="/Users/eduardoalberto/LoadFile/genres", mode="overwrite")  
    total = df.count()

print(f"total de {total} registros carregados")

# file   



