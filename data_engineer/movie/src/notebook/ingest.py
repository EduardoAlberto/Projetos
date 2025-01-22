from pyspark.sql import SparkSession,Row,DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import *

def fn_ingest (df: DataFrame) -> DataFrame :

    dfs = df.limit(15000)\
            .filter(" deathYear <> r'\\N' and deathYear <> r'\\N'  ")\
            .withColumn('profession_array',F.explode((F.split(F.col("primaryProfession"), ","))))\
            .withColumn('knownForTitles_array',F.explode((F.split(F.col("knownForTitles"),","))))\
            .drop_duplicates(['profession_array','knownForTitles_array'])

    
    dff = dfs.filter("profession_array <> r'\\N' and knownForTitles_array <> r'\\N'")
  
    return dff