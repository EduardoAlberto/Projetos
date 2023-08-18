ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.1",
  "org.apache.spark" %% "spark-sql" % "3.2.1",
  "org.apache.spark" %% "spark-mllib" % "3.2.1",
  "org.apache.spark" %% "spark-streaming" % "3.2.1",
  "joda-time" % "joda-time" % "2.10.14",
  "mysql" % "mysql-connector-java" % "8.0.27",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8"



)