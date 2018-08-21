name := "data-engineering-practice"

version := "0.1"

scalaVersion := "2.11.12"


sparkVersion := "2.2.2"
sparkComponents ++= Seq("sql")

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.2"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.2"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.1"



/*
val sparkVersion = "2.2.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % sparkVersion)*/



