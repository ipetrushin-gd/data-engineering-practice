package com.gd.twitteranalytics.reports

import org.apache.spark.sql.{DataFrame, SparkSession}

object TwitterReportGenerator {

  def execute(reportProcessor: (SparkSession, DataFrame, String) =>
    DataFrame, spark: SparkSession, tweetStatusDf: DataFrame, reportDate:String) =
    reportProcessor(spark, tweetStatusDf,reportDate)
}