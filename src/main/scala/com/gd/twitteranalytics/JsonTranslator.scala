package com.gd.twitteranalytics

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object JsonTranslator {
  val log = Logger.getLogger(JsonTranslator.getClass.getName)

  def getParsedJsonAsDataFrame(spark:SparkSession,path:String):DataFrame ={
    import spark.implicits._

    val unParsedDf = spark.read.parquet(path)
    log.debug("=======> Stage 1: Twitter Data Read Start..")
    val parsedJson = unParsedDf.map(_.getAs[String](0))
    spark.read.json(parsedJson)
  }

  def getFilteredDf(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    import spark.implicits._
    tweetStatusDf.select($"user.location",$"id")
  }

}
