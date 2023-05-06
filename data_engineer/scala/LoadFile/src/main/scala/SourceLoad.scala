import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import java.util.Properties
import java.io.File

object SourceLoad {
  def main(args: Array[String])= {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local")
      .getOrCreate()

    val path = "/Users/eduardoalberto/LoadFile/BR-Football-Dataset.csv"
    //valida se arquivo existe
    val arq = new java.io.File(path).exists()
    if(arq){
      val file = path
      val df00 = spark.read.option("delimiter", ",").option("header", "true").csv(file)

    }




  }

}


