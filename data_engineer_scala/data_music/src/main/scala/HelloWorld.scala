package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object HelloWorld {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val path = "/Users/eduardoalberto/LoadFile/animal.txt"
    val lines = spark.read.option("delimiter", "\t").option("header", "true").csv(path)
    lines.show()

  }
}
