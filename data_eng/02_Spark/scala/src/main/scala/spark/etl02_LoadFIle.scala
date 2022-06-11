package spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType, BooleanType}

object etl02_LoadFIle {
  //Create Class
  case class inf_games( Title: String,
                        Features_Handheld: Boolean,
                        Features_Max_Players: Int,
                        Features_Multiplatform: Boolean,
                        Features_Online: Boolean,
                        Metadata_Genres: String,
                        Metadata_Licensed: Boolean,
                        Metadata_Publishers: String,
                        Metadata_Sequel: Boolean,
                        Metrics_Review_Score: Int,
                        Metrics_Sales: Float,
                        Metrics_Used_Price: Float,
                        Release_Console: String,
                        Release_Rating: String,
                        Release_Re_release: Boolean,
                        Release_Year: Int,
                        Length_All_PlayStyles_Average: Float,
                        Length_All_PlayStyles_Leisure: Float,
                        Length_All_PlayStyles_Median: Float,
                        Length_All_PlayStyles_Polled: Int,
                        Length_All_PlayStyles_Rushed: Float,
                        Length_Completionists_Average: Float,
                        Length_Completionists_Leisure: Float,
                        Length_Completionists_Median: Float,
                        Length_Completionists_Polled: Int,
                        Length_Completionists_Rushed: Float,
                        Length_Main_Extras_Average: Float,
                        Length_Main_Extras_Leisure: Float,
                        Length_Main_Extras_Median: Float,
                        Length_Main_Extras_Polled: Int,
                        Length_Main_Extras_Rushed: Float,
                        Length_Main_Story_Average: Float,
                        Length_Main_Story_Leisure: Float,
                        Length_Main_Story_Median: Float,
                        Length_Main_Story_Polled: Int,
                        Length_Main_Story_Rushed: Float)
  def main(args: Array[String]): Unit ={
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
      .add("Title", StringType, nullable = true)
      .add("Features_Handheld", BooleanType, nullable = true)
      .add("Features_Max_Players", IntegerType, nullable = true)
      .add("Features_Multiplatform", BooleanType, nullable = true)
      .add("Features_Online", BooleanType, nullable = true)
      .add("Metadata_Genres", StringType, nullable = true)
      .add("Metadata_Licensed", BooleanType, nullable = true)
      .add("Metadata_Publishers", StringType, nullable = true)
      .add("Metadata_Sequel", BooleanType, nullable = true)
      .add("Metrics_Review_Score", IntegerType, nullable = true)
      .add("Metrics_Sales", FloatType, nullable = true)
      .add("Metrics_Used_Price", FloatType, nullable = true)
      .add("Release_Console", StringType, nullable = true)
      .add("Release_Rating", StringType, nullable = true)
      .add("Release_Re_release", BooleanType, nullable = true)
      .add("Release_Year",  IntegerType, nullable = true)
      .add("Length_All_PlayStyles_Average", FloatType, nullable = true)
      .add("Length_All_PlayStyles_Leisure", FloatType, nullable = true)
      .add("Length_All_PlayStyles_Median", FloatType, nullable = true)
      .add("Length_All_PlayStyles_Polled", IntegerType, nullable = true)
      .add("Length_All_PlayStyles_Rushed", FloatType, nullable = true)
      .add("Length_Completionists_Average", FloatType, nullable = true)
      .add("Length_Completionists_Leisure", FloatType, nullable = true)
      .add("Length_Completionists_Median", FloatType, nullable = true)
      .add("Length_Completionists_Polled", IntegerType, nullable = true)
      .add("Length_Completionists_Rushed", FloatType, nullable = true)
      .add("Length_Main_Extras_Average", FloatType, nullable = true)
      .add("Length_Main_Extras_Leisure", FloatType, nullable = true)
      .add("Length_Main_Extras_Median", FloatType, nullable = true)
      .add("Length_Main_Extras_Polled", IntegerType, nullable = true)
      .add("Length_Main_Extras_Rushed", FloatType, nullable = true)
      .add("Length_Main_Story_Average", FloatType, nullable = true)
      .add("Length_Main_Story_Leisure", FloatType, nullable = true)
      .add("Length_Main_Story_Median", FloatType, nullable = true)
      .add("Length_Main_Story_Polled", IntegerType, nullable = true)
      .add("Length_Main_Story_Rushed", FloatType, nullable = true)

    //file .csv
    import spark.implicits._
    val arq = spark.read
      .schema(gameSchema)
      .option("header", "true")
      .csv("01_data/video_games.csv")
      .as[inf_games]

  try{
    // carregando a tabela no banco
    arq.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/mydesenv")
      .option("dbtable","tb_bi_vgame")
      .option("user", "root")
      .option("password","mysql")
      .mode("Append")
      .save()
    }catch{
      case error: Throwable => println("banco mysql indisponivel",error)
    }
  }
}
