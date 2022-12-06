# importing necessary libraries
from pyspark.sql import SparkSession
  
# function to create new SparkSession
def create_session():
  spk = SparkSession.builder \
      .appName("Corona_cases_statewise.com") \
      .getOrCreate()
  return spk
  
# function to create RDD
def create_RDD(sc_obj,data):
  df = sc.parallelize(data)
  return df
  
  
if __name__ == "__main__":
      
  input_data = [("Uttar Pradesh",122000,89600,12238),
          ("Maharashtra",454000,380000,67985),
          ("Tamil Nadu",115000,102000,13933),
          ("Karnataka",147000,111000,15306),
          ("Kerala",153000,124000,5259)]
  
  # calling function to create SparkSession
  spark = create_session()
  
  # creating spark context object
  sc = spark.sparkContext
  
  # calling function to create RDD
  rd_df = create_RDD(sc,input_data)
  
  schema_lst = ["State","Cases","Recovered","Deaths"]
  
  # creating the dataframe using createDataFrame function
  df = spark.createDataFrame(rd_df,schema_lst)
  # printing schema of the dataframe and showing the dataframe
  df.printSchema()
  df.show()
      
  # retrieving the data from the dataframe using collect() 
  df2= df.collect()
  print("Retrieved Data is:-")
  print(df2)  