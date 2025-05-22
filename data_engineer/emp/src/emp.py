from delta import configure_spark_with_delta_pip
import pyspark.sql.functions as F
import os.path as path
import traceback


def ingest(spark,CONF):

    dfs = None
    
    try:
        if path.isfile(CONF["inptFile"]):
            
            kc_house_dt = spark.read.csv(CONF["inptFile"], header=True,inferSchema=True)\
                                    .withColumn("id_number", F.monotonically_increasing_id())\
                                    .withColumn("dt_ref_carga", F.current_date())
            
            dfs = kc_house_dt.select(   F.coalesce(F.col('id'),F.lit(0)).alias('id'),
                                        F.coalesce(F.col('dt_date'),F.lit(0)).alias('dt_date'),
                                        F.coalesce(F.col('price'),F.lit(0)).alias('price'),
                                        F.coalesce(F.col('bedrooms'),F.lit(0)).alias('bedrooms'),
                                        F.coalesce(F.col('bathrooms'),F.lit(0)).alias('bathrooms'),
                                        F.coalesce(F.col('sqft_living'),F.lit(0)).alias('sqft_living'),
                                        F.coalesce(F.col('sqft_lot'),F.lit(0)).alias('sqft_lot'),
                                        F.coalesce(F.col('floors'),F.lit(0)).alias('floors'),
                                        F.coalesce(F.col('waterfront'),F.lit(0)).alias('waterfront'),
                                        F.coalesce(F.col('view'),F.lit(0)).alias('view'),
                                        F.coalesce(F.col('condition'),F.lit(0)).alias('condition'),
                                        F.coalesce(F.col('grade'),F.lit(0)).alias('grade'),
                                        F.coalesce(F.col('sqft_above'),F.lit(0)).alias('sqft_above'),
                                        F.coalesce(F.col('sqft_basement'),F.lit(0)).alias('sqft_basement'),
                                        F.coalesce(F.col('yr_built'),F.lit(0)).alias('yr_built'),
                                        F.coalesce(F.col('yr_renovated'),F.lit(0)).alias('yr_renovated'),
                                        F.coalesce(F.col('zipcode'),F.lit(0)).alias('zipcode'),
                                        F.coalesce(F.col('lat'),F.lit(0)).alias('lat'),
                                        F.coalesce(F.col('long'),F.lit(0)).alias('long'),
                                        F.coalesce(F.col('sqft_living15'),F.lit(0)).alias('sqft_living15'),
                                        F.coalesce(F.col('sqft_lot15'),F.lit(0)).alias('sqft_lot15'),
                                        F.coalesce(F.col('id_number'),F.lit(0)).alias('id_number'),
                                        F.coalesce(F.col('dt_ref_carga'),F.lit('1900-01-01')).alias('dt_ref_carga'))



            dfs.write.format("delta")\
                    .option("overwriteSchema", "true")\
                    .option("path",CONF["path_delta"])\
                    .partitionBy("dt_ref_carga")\
                    .mode("overwrite")\
                    .saveAsTable("default.tb_emp")
            # mysql
            dfs.write.mode("overwrite").jdbc(CONF["JDBC"], CONF["TABLE"],properties={"user": CONF["DB_USER"],"password": CONF["DB_PASSWORD"]})

        else:
            print("arquivo não existe!")

    except Exception as e:
        print(f"Ocorreu o seguinte erro: {e}")
        traceback.print_exc()
    
    return dfs

