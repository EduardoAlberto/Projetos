import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import java.util.Properties

object LoadVotos {
  val num = 1
  val path = "/Users/eduardoalberto/LoadFile/data-"+num+".csv"
  val ldir = "/Users/eduardoalberto/LoadFile/"
  // usuario senha
  val connectionProperties = new Properties()
  connectionProperties.put("user", "sa")
  connectionProperties.put("password", "Numsey@Password!")

  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local")
      .getOrCreate()

    val arq = new java.io.File(path).exists()
    if (arq){
      val df00 = spark.read.option("delimiter", "\t").option("header","true").csv(path)
      val df01 = df00.select(col("tconst"), col("averageRating"),current_date().alias("dt_atualizacao"))
      df01.limit(20000).write.mode(SaveMode.Append).jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_votos", connectionProperties)
      val total = spark.read.jdbc("jdbc:sqlserver://localhost:1433;databaseName=DBDWP511", "tb_votos", connectionProperties).count()
      println("TOTAL DE "+total+" REGISTROS")
    }
    else{
      println("arquivo não encontrado")

    }

  }

}
