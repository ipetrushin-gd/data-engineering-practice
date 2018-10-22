package com.gd.twitteranalytics

import com.gd.twitteranalytics.util.AppConfigReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TwitterReportingJob {

  val log = Logger.getLogger(TwitterReportingJob.getClass.getName)

  def main(args: Array[String]): Unit = {

    val today = java.time.LocalDate.now
    val dataPath = AppConfigReader.getAppConfigurables(0) + "/event_date=" + today
    val spark = setSparkSession

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fs.exists(new Path(dataPath))) {
      val reportSavePath = "twitter-analytics/reporting"

      log.debug("=======> Building Twitter KPI Report for date " + today)

      val tweetsDataFrame = JsonTranslator.getParsedJsonAsDataFrame(spark, dataPath)

      log.debug("=======> Stage 1: Twitter Data Read Complete !")
      val filteredStatusDf = JsonTranslator.getFilteredDf(spark, tweetsDataFrame)

      val reportFromSql = TwitterReportGenerator.getReportWithSqlProcessing(spark, filteredStatusDf)
      val reportFromDf = TwitterReportGenerator.getReportWithDataFrameProcessing(spark, filteredStatusDf)
      val reportFromDs = TwitterReportGenerator.getReportWithDataSetProcessing(spark, filteredStatusDf)

      log.debug("=======> Stage 5: Twitter Report Creation Complete!")

      log.debug("=======> Stage 6: Saving Report on HDFS!")
      TweetsDataLoader.saveReportToHdfs(reportFromSql, reportSavePath + "/sqlReport")
      TweetsDataLoader.saveReportToHdfs(reportFromDf, reportSavePath + "/dataFrameReport")
      TweetsDataLoader.saveReportToHdfs(reportFromDs, reportSavePath + "/dataSetReport")

      spark.stop
    }
    else{
      log.error("Data for Report Creation is not available !")
      spark.stop
      System.exit(1)
    }
  }

  def setSparkSession:SparkSession = {
      SparkSession.builder()
                  .appName("TwitterKPIReport")
                  .getOrCreate()
  }

}
