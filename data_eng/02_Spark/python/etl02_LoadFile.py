import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# conectando mysql
spark = SparkSession.builder \
        .master('local') \
        .appName("Conection Mysql") \
        .config("spark.driver.extraClassPath", "/Users/eduardoalberto/opt/spark/jars/mysql-connector-java-8.0.29.jar") \
        .getOrCreate()

# diretorio file
dirfiler = "/Users/eduardoalberto/LoadFile/video_games.csv"

def main():
    spark.sparkContext.setLogLevel("OFF")
    inf_games = StructType([
         StructField("Title"                        , StringType(), True)
        ,StructField("Features_Handheld"            , BooleanType(), True)
        ,StructField("Features_Max_Players"         , IntegerType(), True)
        ,StructField("Features_Multiplatform"       , BooleanType(), True)
        ,StructField("Features_Online"              , BooleanType(), True)
        ,StructField("Metadata_Genres"              , StringType(), True)
        ,StructField("Metadata_Licensed"            , BooleanType(), True)
        ,StructField("Metadata_Publishers"          , StringType(), True)
        ,StructField("Metadata_Sequel"              , BooleanType(), True)
        ,StructField("Metrics_Review_Score"         , IntegerType(), True)
        ,StructField("Metrics_Sales"                , FloatType(), True)
        ,StructField("Metrics_Used_Price"           , FloatType(), True)
        ,StructField("Release_Console"              , StringType(), True)
        ,StructField("Release_Rating"               , StringType(), True)
        ,StructField("Release_Re_release"           , BooleanType(), True)
        ,StructField("Release_Year"                 , IntegerType(), True)
        ,StructField("Length_All_PlayStyles_Average", FloatType(), True)
        ,StructField("Length_All_PlayStyles_Leisure", FloatType(), True)
        ,StructField("Length_All_PlayStyles_Median" , FloatType(), True)
        ,StructField("Length_All_PlayStyles_Polled" , IntegerType(), True)
        ,StructField("Length_All_PlayStyles_Rushed" , FloatType(), True)
        ,StructField("Length_Completionists_Average", FloatType(), True)
        ,StructField("Length_Completionists_Leisure", FloatType(), True)
        ,StructField("Length_Completionists_Median" , FloatType(), True)
        ,StructField("Length_Completionists_Polled" , IntegerType(), True)
        ,StructField("Length_Completionists_Rushed" , FloatType(), True)
        ,StructField("Length_Main_Extras_Average"   , FloatType(), True)
        ,StructField("Length_Main_Extras_Leisure"   , FloatType(), True)
        ,StructField("Length_Main_Extras_Median"    , FloatType(), True)
        ,StructField("Length_Main_Extras_Polled"    , IntegerType(), True)
        ,StructField("Length_Main_Extras_Rushed"    , FloatType(), True)
        ,StructField("Length_Main_Story_Average"    , FloatType(), True)
        ,StructField("Length_Main_Story_Leisure"    , FloatType(), True)
        ,StructField("Length_Main_Story_Median"     , FloatType(), True)
        ,StructField("Length_Main_Story_Polled"     , IntegerType(), True)
        ,StructField("Length_Main_Story_Rushed"     , FloatType(), True)
        ,StructField("dt_atualizacao"               , DateType(), True)
        ])
    arq = spark.read.csv(dirfiler, header=True, schema=inf_games)
    # add novo campo 
    tp = arq.withColumn("dt_atualizacao", lit(dt.datetime.now()))
    view = tp.select('Title','Features_Max_Players','Features_Online','Features_Multiplatform','Metadata_Genres','Metadata_Publishers','Metrics_Review_Score','Metrics_Sales','Metrics_Used_Price','Release_Year','Release_Rating','dt_atualizacao').show()
    try:
            view.write.format("jdbc").options(
            url="jdbc:mysql://localhost:3306/mydesenv",
            driver = "com.mysql.cj.jdbc.Driver",
            dbtable = "stg_video_game",
            user="root",
            password="mysql").mode('append').save()
    except:
            print('Erro ao tenta carregar banco !')

if __name__ == "__main__":
        main()

