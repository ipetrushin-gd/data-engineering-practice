package com.gd.twitteranalytics

import com.gd.twitteranalytics.TwitterReportingJob.log
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object TwitterReportGenerator {

  def getReportWithSqlProcessing(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    log.debug("=======> Stage 3: Twitter Report via SQL Start.... ")
    tweetStatusDf.createOrReplaceTempView("status")
    val query = "select location,id,count(id) as frequency from status group by location,id having frequency >= 10"
    spark.sql(query).withColumn("Date",lit(current_date)).drop("frequency")
  }

  def getReportWithDataFrameProcessing(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    log.debug("=======> Stage 4: Twitter Report via SQL Start.... ")
    import spark.implicits._

    tweetStatusDf.select($"location",$"id").
                  groupBy("location","id") .
                  agg(count($"id")as "frequency").where(count($"id") geq 10).
                  withColumn("Date",lit(current_date)).drop("frequency")
  }

  def getReportWithDataSetProcessing(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    log.debug("=======> Stage 5: Twitter Report via SQL Start.... ")
    import spark.implicits._

    val encoder = Encoders.product[TwitterStatus]
    val tweetDs:Dataset[TwitterStatus] = tweetStatusDf.as[TwitterStatus](encoder)
        tweetDs.select($"location",$"id").
                groupBy("location","id") .
                agg(count($"id")as "frequency").where(count($"id") geq 10).
                withColumn("Date",lit(current_date)).drop("frequency")
  }
}

case class TwitterStatus(location:String,id:Long)
