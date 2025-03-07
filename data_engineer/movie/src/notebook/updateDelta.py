from pyspark.sql import SparkSession,Row,DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *
import traceback



import os.path as path


def LaodDelta (spark, dir):
    print()

    try:
        path.exists(dir)
        df = spark.read.option("delimiter",';').option("header", "True").option("inferSchema", "True").csv(dir)
        df01 = df.groupBy("name","imdb_id","overview","revenue","runtime","status","title","vote_average","vote_count","popularity","name_geners")\
                 .agg(F.count("title").alias("total"))\
                 .withColumn("dt_ref_carga", F.current_date())

        df02 = df.groupBy("name","imdb_id","overview","revenue","runtime","status","title","vote_average","vote_count","popularity","name_geners")\
                 .agg(F.count("title").alias("total"))\
                 .withColumn("dt_ref_carga", F.current_date()+2)
    
        df01.write.format("delta").mode("overwrite").partitionBy("dt_ref_carga").option("overwriteSchema", "true").saveAsTable("tb_ratingMovie")
        df02.write.format("delta").mode("overwrite").partitionBy("dt_ref_carga").option("overwriteSchema", "true").saveAsTable("st_ratingMovie")
 
    except Exception as e:
        print(f"Ocorreu o seguinte erro: {e}")
        traceback.print_exc()
    return None


def MergeDelta(dftg,dfsr):
    try:
        dftg.alias("dftg")\
            .merge(dfsr.alias("dfsr"),"dftg.imdb_id = dfsr.imdb_id")\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .whenNotMatchedBySourceDelete()\
            .execute()
        
    except Exception as e:
        print(f"Ocorreu o seguinte erro: {e}")
        traceback.print_exc()
    return None