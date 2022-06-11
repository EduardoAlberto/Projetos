package spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}

object etl01_LoadFile {
  //create class
  case class game( Rank: String
                  ,Name: String
                  ,Platform: String
                  ,Year: Int
                  ,Genre: String
                  ,Publisher: String
                  ,NA_Sales: Float
                  ,EU_Sales: Float
                  ,JP_Sales: Float
                  ,Other_Sales: Float
                  ,Global_Sales: Float)

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Use SparkSession
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local")
      .getOrCreate()

    //DataType in variable
    val gameSchema = new StructType()
      .add("Rank", StringType, nullable = true)
      .add("Name", StringType, nullable = true)
      .add("Platform", StringType, nullable = true)
      .add("Year", IntegerType, nullable = true)
      .add("Genre", StringType, nullable = true)
      .add("Publisher", StringType, nullable = true)
      .add("NA_Sales", FloatType, nullable = true)
      .add("EU_Sales", FloatType, nullable = true)
      .add("JP_Sales", FloatType, nullable = true)
      .add("Other_Sales", FloatType, nullable = true)
      .add("Global_Sales", FloatType, nullable = true)

    //file .csv
    import spark.implicits._
    val arq = spark.read
      .schema(gameSchema)
      .csv("01_data/vgsales.csv")
      .as[game]

    //show database mysql up
    try{
      // verifica se tem conexao
      val dataframe_mysql = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mydesenv")
      .option("dbtable","tb_load")
      .option("user", "root")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("password","mysql")
      .load()
      .show()

      // carregando a tabela no banco
      arq.write.format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/mydesenv")
        .option("dbtable","tb_bi_video_game")
        .option("user", "root")
        .option("password","mysql")
        .mode("Ignore")
        .save()

    } catch{
      case error : Throwable => println("banco mysql esta fora", error)
    }
  }

}
