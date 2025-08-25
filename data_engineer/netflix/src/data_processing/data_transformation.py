# data_processing/batch_transformations.py
from pyspark.sql import DataFrame, functions as F
# No início do batch_transformations.py
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent.parent))  # Adiciona DTM/ ao path
from config import settings

class DataTransformer:
    def __init__(self):
        pass

    def apply_transformations(self, df: DataFrame) -> DataFrame:
        df = (
                df.withColumn("id", F.substring(F.col("show_id"), 2, 4))
                .withColumn("date_added", F.trim(F.col("date_added")))
                .withColumn("dt_added",F.when(F.col("date_added").rlike("^[A-Za-z]+ [0-9]{1,2}, [0-9]{4}$"), F.to_date(F.col("date_added"), "MMMM d, yyyy")).otherwise(None))
                .withColumn("dt_processamento", F.current_timestamp())  
            ) 

        return df
    
    def country_transformations(self, df: DataFrame) -> DataFrame:
        df02 = (
            df.groupBy("country", "id")
              .agg(F.count("show_id").alias("total_shows"))
              .orderBy(F.desc("total_shows"))
              .withColumnRenamed("country", "country_name")
        )
            
        df03 = (
            df.alias("n1")
              .join(df02.alias("n2"), on=["id"], how="left")
              .na.drop(subset=["country", "cast", "director"])
        )
        
        return df03