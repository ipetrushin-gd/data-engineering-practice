name := "data-engineering-practice"

version := "0.1"

scalaVersion := "2.11.12"

sparkVersion := "2.2.2"
sparkComponents ++= Seq("sql")

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.2" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3"

//libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.1"



