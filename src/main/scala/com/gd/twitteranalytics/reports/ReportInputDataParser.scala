package com.gd.twitteranalytics.reports

import org.apache.spark.sql.{DataFrame, SparkSession}

object  ReportInputDataParser{

  def getPayloadStatusAsDataFrame(spark:SparkSession,path:String):DataFrame ={
    import spark.implicits._
    val unparsedDf = spark.read.parquet(path)
    val parsedJson = unparsedDf.map(_.getAs[String](0))
    spark.read.json(parsedJson)
  }

  def getUserIdAndLocation(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    import spark.implicits._
    tweetStatusDf.select($"user.location",$"id").na.fill("NOT_AVAILABLE")
  }
}
