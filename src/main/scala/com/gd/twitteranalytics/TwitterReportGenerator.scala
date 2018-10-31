package com.gd.twitteranalytics

import org.apache.spark.sql.{DataFrame,SparkSession}

object TwitterReportGenerator {

  def execute(reportProcessor: (SparkSession, DataFrame) => DataFrame,
              spark: SparkSession, tweetStatusDf: DataFrame) = reportProcessor(spark, tweetStatusDf)
}