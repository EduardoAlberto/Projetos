import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object version {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder()
      .appName("Ingest Netflix")
      .master("local")
      .getOrCreate()
    println("Version Spark : "+ spark.version )
    println("Version Spark : "+ spark.sparkContext.version )

    spark.sparkContext.setLogLevel("ERROR")


  }
}
