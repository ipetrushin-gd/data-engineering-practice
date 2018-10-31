package com.gd.twitteranalytics

import util.AppConfigReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import ReportProcessor._

object TwitterReportingJob {

  val log = Logger.getLogger(TwitterReportingJob.getClass.getName)

  def main(args: Array[String]): Unit = {

    val today = java.time.LocalDate.now
    val dataPath = AppConfigReader.getAppConfigurables(0) + "/event_date=" + today
    val spark = setSparkSession
    val reportSavePath = AppConfigReader.getAppConfigurables(3)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if(fs.exists(new Path(dataPath))) {

      log.debug("=======> Building Twitter KPI Report for date " + today)

      val tweetsDataFrame = JsonTranslator.getParsedJsonAsDataFrame(spark, dataPath)

      log.debug("=======> Stage 1: Twitter Data Read Complete !")
      val filteredStatusDf = JsonTranslator.getFilteredDf(spark, tweetsDataFrame)

      log.debug("=======> Stage 2: Generating Report for Active Users..")
      val reportFromSql = TwitterReportGenerator.execute(getReportWithSqlProcessing,spark,filteredStatusDf)
      val reportFromDf = TwitterReportGenerator.execute(getReportWithDataFrameProcessing,spark,filteredStatusDf)
      val reportFromDs = TwitterReportGenerator.execute(getReportWithDataSetProcessing,spark,filteredStatusDf)

      log.debug("=======> Stage 3: Twitter Report Creation Complete!")

      log.debug("=======> Stage 4: Saving Report on HDFS!")
      TwitterReportWriter.saveReportToHdfs(reportFromSql, reportSavePath + "/sqlReport")
      TwitterReportWriter.saveReportToHdfs(reportFromDf, reportSavePath + "/dataFrameReport")
      TwitterReportWriter.saveReportToHdfs(reportFromDs, reportSavePath + "/dataSetReport")

      spark.stop
    }
    else{
      log.error("Data for Report Creation is not available !")
      spark.stop
      System.exit(1)
    }
  }
  def setSparkSession:SparkSession = {
      SparkSession.builder().appName("TwitterKPIReport").getOrCreate()
  }
}
