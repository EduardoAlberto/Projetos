ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.1.0",
  "org.apache.spark" %% "spark-sql" % "3.1.0",
  "org.apache.spark" %% "spark-mllib" % "3.1.0",
  "org.apache.spark" %% "spark-streaming" % "3.1.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4"
)


