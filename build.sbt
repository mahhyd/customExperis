name := "customerExperis"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.1",
  "org.apache.spark" %% "spark-sql" % "2.4.1",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test
)