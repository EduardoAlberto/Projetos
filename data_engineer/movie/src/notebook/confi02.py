# %%
from pyspark.sql import SparkSession,Row,DataFrame
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from updateDelta import LaodDelta, MergeDelta

builder = SparkSession \
    .builder \
    .appName("Data with Nikk the Greek Spark Session") \
    .master("local[4]") \
    .config("spark.jars.packages", "uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.5") \
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.warehouse.dir", "/Users/eduardoalberto/LoadFile/repository/deltaTable/") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

spark = configure_spark_with_delta_pip(builder).getOrCreate()

sc = spark.sparkContext
spark.sparkContext.setLogLevel("OFF") 
print('PySpark Version :'+spark.version)
print('PySpark Version :'+spark.sparkContext.version)

spark

# %%
dftg = DeltaTable.forPath(spark, "/Users/eduardoalberto/LoadFile/repository/deltaTable/tb_ratingmovie")
dfsr = spark.read.format("delta").load("/Users/eduardoalberto/LoadFile/repository/deltaTable/st_ratingmovie")


dir = "/Users/eduardoalberto/LoadFile/part-00000-055103f0-b275-4e27-b667-0c2c25d0636a-c000.csv"

LaodDelta(spark, dir)
MergeDelta(dftg,dfsr)

# %%
