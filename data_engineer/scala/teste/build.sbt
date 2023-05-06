ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.2",
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-mllib" % "3.3.2",
  "org.apache.spark" %% "spark-streaming" % "3.3.2",
  "joda-time" % "joda-time" % "2.12.2",
  "mysql" % "mysql-connector-java" % "8.0.32",
  "com.microsoft.sqlserver" % "mssql-jdbc" % "7.4.1.jre8"

)