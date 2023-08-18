# ### arquivo .csv

# %%
# Import PySpark
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession
spark=SparkSession.builder.master("local[1]")\
        .appName("Music")\
        .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark/jars/mssql-jdbc-7.4.1.jre8.jar" ) \
        .getOrCreate()
sc = spark.sparkContext
spark.sparkContext.setLogLevel("OFF") 
print('PySpark Version :'+spark.version)
# print('PySpark Version :'+spark.sparkContext.version)

txt = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/data.csv')

# %%
class dados():
    def __init__(self,txt):
        self.__txt  = txt


    def music(self):
        df = self.__txt.select(col("id")
                ,col("acousticness").cast(DecimalType(3,2))
                ,regexp_replace(regexp_replace(regexp_replace(col("artists"),r"\[",""),r"\]",""),"'","").alias("artists")
                ,col("danceability").cast(DecimalType(3,2))
                ,col("duration_ms")
                ,col("energy").cast(DecimalType(3,2))
                ,col("explicit")
                ,col("instrumentalness").cast(DecimalType(3,2))
                ,col("key")
                ,col("liveness").cast(DecimalType(3,2))
                ,col("loudness").cast(DecimalType(5,2))
                ,col("name")
                ,col("popularity")
                ,when(col("release_date")=="1928",lit("1928-01-01")).otherwise(col("release_date")).alias("release_date")
                ,col("speechiness").cast(DecimalType(5,2))
                ,col("tempo").cast(DecimalType(6,3))
                ,col("valence").cast(DecimalType(5,2))
                ,col("year")
                )
        return df

# %%
txt = spark.read.option("header", "true").option('inferSchema', 'true').csv('/Users/eduardoalberto/LoadFile/data.csv')
x = dados(txt)
df = x.music()
df.write.mode("overwrite").jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_music",properties={"user": "sa", "password": "Numsey@Password!"})
print('#'*55)
df.groupBy('artists').count().orderBy(desc('count')).show(truncate=False)
print('#'*55)


# %%



