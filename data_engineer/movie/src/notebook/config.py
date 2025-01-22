# %%
import pyspark 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from  ingest import fn_ingest


spark=SparkSession.builder.master("local[1]")\
        .appName("movie_local")\
        .config("spark.driver.extraClassPath","/Users/eduardoalberto/opt/spark-3.5.4/jars/mysql-connector-j-9.1.0.jar")\
        .config("spark.sql.shuffle.partitions", "7") \
        .config("spark.sql.warehouse.dir", "/Users/eduardoalberto/LoadFile/repository/database/") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.executor.cores", "4") \
        .config("spark.dynamicAllocation.minExecutors", "1") \
        .config("spark.dynamicAllocation.maxExecutors", "5") \
        .enableHiveSupport()\
        .getOrCreate()
spark.sparkContext.setLogLevel("OFF") 
print('PySpark Version :'+spark.version)
print('PySpark Version :'+spark.sparkContext.version)

# %%
arq = spark.read.option("delimiter", "\t").option("header", True).csv('/Users/eduardoalberto/LoadFile/data_20220920.csv')

dfs = fn_ingest(arq)

dfs.write.mode("overwrite").jdbc("jdbc:mysql://localhost:3306/myDbUser", "myDbUser.movie",properties={"user": "root", "password": "mysql"})


# %%
