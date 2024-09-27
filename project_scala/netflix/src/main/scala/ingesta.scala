import org.apache.spark.sql._
import org.apache.log4j._

class loadfile(spark: SparkSession){
  def netflix(arq:String): DataFrame ={
    val df = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(arq)
    df
  }
}

object ingest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Ingest Netflix")
      .master("local")
      .getOrCreate()


    val txt = new loadfile(spark)
    val dir = "/Users/eduardoalberto/LoadFile/netflix_titles.csv"
    val df = txt.netflix(dir)
    df.show()

    spark.sparkContext.setLogLevel("ERROR")
    //spark.stop()
  }

}