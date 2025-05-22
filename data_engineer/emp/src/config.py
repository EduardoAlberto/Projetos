from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from emp import ingest


builder = SparkSession.builder \
    .appName("emp") \
    .master("local[4]") \
    .config("spark.jars.packages", 
            "io.delta:delta-core_2.12:3.0.0,"
            "uk.co.gresearch.spark:spark-extension_2.12:2.11.0-3.5,"
            "org.apache.spark:spark-avro_2.12:3.5.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "/Users/eduardoalberto/LoadFile/output/") \
    .config("spark.driver.extraClassPath", "/Users/eduardoalberto/opt/spark-3.5.3/jars/mysql-connector-j-9.1.0.jar")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

sc = spark.sparkContext
spark.sparkContext.setLogLevel("OFF") 
print('PySpark Version :'+spark.version)


CONF = {'inptFile': '/Users/eduardoalberto/LoadFile/input/kc_house_data.csv',
        'path_delta': '/Users/eduardoalberto/LoadFile/output/emp',
        'JDBC':  "jdbc:mysql://localhost:3306/myDbUser",
        'TABLE': "myDbUser.emp",
        'DB_USER': 'root',
        'DB_PASSWORD': 'mysql'
    }

print(30*'#'+'  INICIO CARGA '+'#'*30)

df = ingest(spark,CONF)
print('Total de registros carregados = ' + str(df.count()))

print("Carga finalizada com sucesso")
print(30*'#'+'  FIM '+'#'*30)
spark.stop()