package com.gd.twitteranalytics.reports

import org.apache.spark.sql.{DataFrame, SparkSession}

object TwitterReportGenerator {

  def execute(f: (SparkSession, DataFrame) => DataFrame, spark: SparkSession, tweetStatusDf: DataFrame) = f(spark, tweetStatusDf)

}
