package com.gd.twitteranalytics.reports

import com.gd.twitteranalytics.reports.TwitterReportGenerator.execute
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

trait ActiveUsersReport extends TwitterReport {

  private val logger = Logger.getLogger(getClass)

  override def getOutputDatasetPath(): String =
    s"$BaseReportsPath/ActiveUsers/$getReportType"

  def getReportBuildingStrategy()
    : (SparkSession, DataFrame, String) => DataFrame

  override def run(spark: SparkSession): Unit = {
    if (validateSourceDataset(spark)) {
      logger.debug(
        "=======> Stage 1: Reading Data for Twitter Active User Report...")
      val inputDataForReport = getInputData(getSourceDatasetPath(), spark)

      logger.debug(
        "=======> Stage 2: Creating report for Twitter Active Users...")
      val report =
        execute(getReportBuildingStrategy(),
                spark,
                inputDataForReport,
                getReportDate())

      logger.debug("=======> Stage 3: Saving Report on HDFS...")
      saveReport(report)

      spark.stop
    } else {
      stopJob(spark)
    }
  }

  override def getSourceDatasetPath(): String = {
    s"$DataLakeTweetsPath/$DataLakeEventsPartitionKey=${getReportDate()}"
  }

  def getInputData(dataPath: String, spark: SparkSession): DataFrame = {
    val tweetsDataFrame =
      ReportInputDataParser.getPayloadStatusAsDataFrame(spark, dataPath)
    ReportInputDataParser.getUserIdAndLocation(spark, tweetsDataFrame)
  }
}
