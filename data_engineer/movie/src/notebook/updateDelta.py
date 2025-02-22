from pyspark.sql import SparkSession,Row,DataFrame
import pyspark.sql.functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql.types import *



def LaodDelta (self,spark) :
    df01 = df.groupBy("name","imdb_id","overview","revenue","runtime","status","title","vote_average","vote_count","popularity","name_geners")\
             .agg(F.count("title").alias("total"))\
             .withColumn("dt_ref_carga", F.current_date())

    df02 = df.groupBy("name","imdb_id","overview","revenue","runtime","status","title","vote_average","vote_count","popularity","name_geners")\
             .agg(F.count("title").alias("total"))\
             .withColumn("dt_ref_carga", F.current_date()+2)
    
    df01.write.format("delta").mode("overwrite").partitionBy("dt_ref_carga").option("overwriteSchema", "true").saveAsTable("tb_ratingMovie")
    df02.write.format("delta").mode("overwrite").partitionBy("dt_ref_carga").option("overwriteSchema", "true").saveAsTable("st_ratingMovie")

def MergeDelta(df: DataFrame) -> DataFrame :
    dftg = DeltaTable.forPath(spark, "/Users/eduardoalberto/LoadFile/repository/deltaTable/tb_ratingmovie")
    dfsr = spark.read.format("delta").load("/Users/eduardoalberto/LoadFile/repository/deltaTable/st_ratingmovie")


    (dftg.alias("dftg")
        .merge(dfsr.alias("dfsr"),"dftg.imdb_id = dfsr.imdb_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
