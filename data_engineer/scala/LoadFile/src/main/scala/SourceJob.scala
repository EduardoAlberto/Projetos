import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import java.util.Properties

object SourceJob {
    def main(args: Array[String]): Unit = {
      // connect mssql
      val connectionProperties = new Properties()
      connectionProperties.put("user", "sa")
      connectionProperties.put("password", "Numsey@Password!")
      //Variavel de erro
      Logger.getLogger("org").setLevel(Level.ERROR)
     // criar set a variavel
      val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local")
        .getOrCreate()
      val path = "/Users/eduardoalberto/LoadFile/archive/genres_v2.csv"
      val file = new java.io.File(path).exists()
        if (file) {
            val dfs = path
            val df1 =  spark.read.option("delimiter", ",").option("header", "true").option("inferSchema", "true").csv(dfs)
            val df2 = df1.select(col("id"),
                                 col("key"),
                                 col("genre"),
                                 col("song_name"),
                                 col("title"),
                                 format_number(col("tempo"),2).alias("tempo"),
                                 format_number(col("acousticness"),2).alias("acousticness"),
                                 format_number(col("instrumentalness"),2).alias("instrumentalness"))

            val df3 = df2.withColumn("new_title", when(col("title").isNull, split(col("song_name")," ").getItem((0))))
            df3.limit(200000).partitionBy("tempo").write.mode(SaveMode.Append).jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_atores", connectionProperties)



        }
    }

}
