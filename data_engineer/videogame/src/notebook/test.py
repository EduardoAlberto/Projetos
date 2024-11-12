from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

print(20*'#')
print('PySpark Version :'+spark.version)
print('PySpark Version :'+spark.sparkContext.version)
print('spark executado Ok')
print(20*'#')

