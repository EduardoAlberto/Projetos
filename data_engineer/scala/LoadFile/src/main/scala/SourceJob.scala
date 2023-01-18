import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object SourceJob {
  def main(args: Array[String]): Unit = {
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

        val df3 = df2.withColumn("new_title", when(col("title").isNull, split(col("song_name")," ").getItem((0)))).show()
//        val df4 = df3.select(col("song_name"),split(col("new_title")," ").getItem(0)).show()

    }


  }


}
