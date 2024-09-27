ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "netflix"
  )
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.3"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"