from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import mysql.connector

# conectando mysql
spark_mysql= SparkSession.builder \
        .master('local') \
        .appName("Conection Mysql") \
        .config("spark.driver.extraClassPath", "/Users/eduardoalberto/opt/spark-3.2.1-bin-hadoop3.2/jars/mysql-connector-java-8.0.28.jar") \
        .getOrCreate()