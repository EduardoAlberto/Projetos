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
dirfiler = "/Users/eduardoalberto/LoadFile/vgsales.csv"

def main():
        # Set the log level to only print errors
        spark.sparkContext.setLogLevel("OFF") 

        # set DataType in variable
        gameSchema = StructType([ 
                 StructField("Rank", StringType(), True) 
                ,StructField("Name", StringType(), True) 
                ,StructField("Platform", StringType(), True)
                ,StructField("Year", IntegerType(), True) 
                ,StructField("Genre", StringType(), True) 
                ,StructField("Publisher", StringType(), True)
                ,StructField("NA_Sales", FloatType(), True) 
                ,StructField("EU_Sales", FloatType(), True) 
                ,StructField("JP_Sales", FloatType(), True) 
                ,StructField("Other_Sales", FloatType(), True) 
                ,StructField("Global_Sales", FloatType(), True) 
                ,StructField("dt_atualizacao", DateType(), True) 
                
                ])
        # read file
        arq = spark.read.csv(dirfiler, header=True, schema=gameSchema)
        # add variavel data atualização 
        tp = arq.withColumn("dt_atualizacao", lit(dt.datetime.now()))
        try:
                tp.write.format("jdbc").options(
                url="jdbc:mysql://localhost:3306/mydesenv",
                driver = "com.mysql.cj.jdbc.Driver",
                dbtable = "stg_list_video_game",
                user="root",
                password="mysql").mode('append').save()
        except:
                print('Erro ao tenta carregar banco !')
           
        tp.show()
if __name__ == "__main__":
        main()



        


   
