import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, StringType, StructType}

class dados(spark: SparkSession) {
  def music(arq01: String): DataFrame = {
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(arq01)

      df
  }

}

object Script {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("Script de Carga")
      .master("local")
      .getOrCreate()

    val TXT = new dados(spark)
    val caminho = "/Users/eduardoalberto/LoadFile/data.csv"
    val df = TXT.music(caminho)
    df.show()

    spark.stop()
  }
}




