import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StructType}
import java.util.Properties
import java.io.File

object LoadAtores {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local")
      .getOrCreate()

    // arquivos
    val path = "/Users/eduardoalberto/LoadFile/data-0.csv"
    // usuario senha
    val connectionProperties = new Properties()
    connectionProperties.put("user", "sa")
    connectionProperties.put("password", "Numsey@Password!")
    //ETL tb_atores
    val arq = new java.io.File(path).exists()
    if (arq) {
      val path00 = path
      val df00 = spark.read.option("delimiter", "\t").option("header", "true").csv(path00)
      val df01 = df00.select(col("nconst")
        , col("primaryName")
        , col("birthYear")
        , col("primaryProfession")
        , when(col("deathYear") === "\\N", "0000").otherwise(col("deathYear")).alias("deathYear")
        , substring(col("knownForTitles"), 1, 9).alias("id001")
        , substring(col("knownForTitles"), 11, 9).alias("id002")
        , substring(col("knownForTitles"), 21, 9).alias("id003")
        , current_date().alias("dt_atualizacao"))
      df01.limit(200000).write.mode(SaveMode.Append).jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_atores", connectionProperties)
      val total = df01.count()
      println("Total de registros: " + total)
    }
    else {
      println("arquivo não encontrado")
    }

  }

}
