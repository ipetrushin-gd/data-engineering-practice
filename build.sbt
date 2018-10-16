name := "twitter-analytics"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.2"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.2.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.11.1"
libraryDependencies += "com.typesafe" % "config"               % "1.3.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.1.0" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.3.1_0.10.0" % "test"
libraryDependencies += "org.apache.spark" %% "spark-hive"       % "2.0.0" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
