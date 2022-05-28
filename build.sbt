name := "StructuredStreaming"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.12.10"
autoScalaLibrary := falsea
val sparkVersion = "3.0.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
)

libraryDependencies ++= sparkDependencies