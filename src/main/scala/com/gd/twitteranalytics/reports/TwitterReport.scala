package com.gd.twitteranalytics.reports

import com.gd.twitteranalytics.reports.ReportType.ReportType
import com.gd.twitteranalytics.reports.TwitterReportWriter.saveReportToHdfs
import com.gd.twitteranalytics.util.AppConfigReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

trait TwitterReport {

  val DataLakeEventsPartitionKey = "event_date"
  val DataLakeTweetsPath = AppConfigReader.getAppConfigurables(0)
  val BaseReportsPath = AppConfigReader.getReportConfigurables(0)

  private val logger = Logger.getLogger(getClass)
  private var errorMessage: String = _

  def getReportName(): String

  def getReportType(): ReportType

  def getReportDate() = ExternalConfigLoader.getEventDateForReportCreation

  def getSourceDatasetPath(): String

  def getOutputDatasetPath(): String

  def validateSourceDataset(spark: SparkSession): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(new Path(getSourceDatasetPath()))) {
      errorMessage = "Source / input data for report creation is not available!"
      false
    } else
      true
  }

  def getSparkSession(): SparkSession = {
    SparkSession.builder.appName(getReportName()).getOrCreate
  }

  def saveReport(report: DataFrame) = {
    saveReportToHdfs(report, getOutputDatasetPath())
  }

  def stopJob(spark: SparkSession) = {
    if (!errorMessage.isEmpty)
      logger.error(errorMessage)

    spark.stop
    System.exit(1)
  }

  def run(spark: SparkSession)
}
