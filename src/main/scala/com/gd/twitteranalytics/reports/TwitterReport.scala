package com.gd.twitteranalytics.reports

import com.gd.twitteranalytics.reports.TwitterReportWriter.saveReportToHdfs
import com.gd.twitteranalytics.util.AppConfigReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

trait TwitterReport {

  val today = java.time.LocalDate.now
  val dataPath = AppConfigReader.getAppConfigurables(0) + "/event_date=" + AppConfigReader.getReportConfigurables(1)
  val activeUserReportSavePath = AppConfigReader.getReportConfigurables(0) + "/ActiveUsers"
  var errorMessage = ""

  def validateReportPath(spark: SparkSession,log:Logger):Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path(dataPath))){
      errorMessage = errorMessage.concat("Data for Report Creation is not available !")
      false
    }
    else {
      log.debug("======> Stage 1: Building Report for Twitter Performance...")
      true
    }
  }

  def getInputDataForActiveUserReport(dataPath:String,spark:SparkSession,log:Logger):DataFrame = {
    log.debug("=======> Stage 2: Reading Data for Twitter Active User Report")
    val tweetsDataFrame = ReportInputDataParser.getPayloadStatusAsDataFrame(spark, dataPath)
    ReportInputDataParser.getUserIdAndLocation(spark, tweetsDataFrame)
  }

  def getLogger(className:String):Logger = {
    Logger.getLogger(className.getClass.getName)
  }

  def setSparkSession(name:String):SparkSession = {
    SparkSession.builder.appName(name).master("local[*]").getOrCreate
  }

  def printErrorLogs(log:Logger) = {
    if (!errorMessage.isEmpty)
      log.error(errorMessage)
  }

  def saveActiveUserReport(report:DataFrame,reportType:String,log:Logger)={
    log.debug("=======> Stage 3: Saving Report on HDFS...")
    saveReportToHdfs(report, activeUserReportSavePath + reportType)
  }
}
