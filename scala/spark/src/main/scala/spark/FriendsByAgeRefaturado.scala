package spark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession


object FriendsByAgeRefaturado {


  def main(args: Array[String]): Unit ={


    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "FriendsByAge")
    val lines = sc.textFile("data/fakefriends-noheader.csv")

    //print
    //lines.foreach(println)
    val age = lines.map(x => x.split(",")(2).toInt)
    val numFriends = lines.map(x => x.split(",")(3).toInt)

//    val a = age.foreach(println)
//    val b = numFriends.foreach(println)

//    val spark = SparkSession.builder.getOrCreate()
//
//    val columns = Seq(age, numFriends)
//    val rdd = spark.sparkContext.parallelize(columns, 1)

//    val totalsByAge = age.map(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))





//    numFriends.foreach(println)


//    data.foreach(println)



  }


}
