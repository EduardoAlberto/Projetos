import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import java.util.Properties
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType, StringType, StructType}

object cargaMusic {
  case class arquvivo (acousticness: String
                      ,artists: String
                      ,danceability: String
                      ,duration_ms: String
                      ,energy: String
                      ,explicit: String
                      ,id: String
                      ,instrumentalness: String
                      ,key: String
                      ,liveness: String
                      ,loudness: String
                      ,mode: String
                      ,name: String
                      ,popularity: String
                      ,release_date: String
                      ,speechiness: String
                      ,tempo: String
                      ,valence: String
                      ,year: String)

  def main(args: Array[String]){
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    //Use SparkSession
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local")
      .getOrCreate()

    // Defina as informações de conexão ao SQL Server
    val jdbcHostname = "localhost"
    val jdbcPort = 1433 // Porta padrão do SQL Server
    val jdbcDatabase = "DBDWP511"
    val jdbcUsername = "sa"
    val jdbcPassword = "Numsey@Password!"

    // Crie a URL de conexão JDBC
    val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase};user=${jdbcUsername};password=${jdbcPassword}"

    // Defina as propriedades de conexão
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", jdbcUsername)
    connectionProperties.setProperty("password", jdbcPassword)
    connectionProperties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")


    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("/Users/eduardoalberto/LoadFile/data.csv")
      .as[arquvivo]
    // Transformation variable
    val dfp = people.select(col("id")
                           ,col("acousticness").cast(DecimalType(3,2))
                           ,regexp_replace(regexp_replace(regexp_replace(col("artists"),"\\[",""),"\\]",""),"'","").alias("artists")
                           ,col("danceability").cast(DecimalType(3, 2))
                           ,col("duration_ms")
                           ,col("energy").cast(DecimalType(3, 2))
                           ,col("explicit")
                           ,col("instrumentalness").cast(DecimalType(3, 2))
                           ,col("key")
                           ,col("liveness").cast(DecimalType(3, 2))
                           ,col("loudness").cast(DecimalType(5, 2))
                           ,col("name")
                           ,col("popularity")
                           ,when(col("release_date") === "1928", lit("1928-01-01")).otherwise(col("release_date")).alias("release_date")
                           ,col("speechiness").cast(DecimalType(5, 2))
                           ,col("tempo").cast(DecimalType(6, 3))
                           ,col("valence").cast(DecimalType(5, 2))
                           ,col("year"))
    // Escreva os dados no SQL Server
    dfp.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "tb_music02", connectionProperties)

    val total = dfp.groupBy("artists").count().orderBy(desc("count"))
    println("//" * 55)
    total.show()
    println("Fim do processo !!")
    println("//" * 55)

    // Encerre a sessão Spark
    spark.stop()


  }


}
