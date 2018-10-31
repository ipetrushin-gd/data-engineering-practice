package com.gd.twitteranalytics.reports

import ReportProcessor._
import TwitterReportGenerator.execute
import TwitterReportWriter.saveReportToHdfs
import org.apache.hadoop.fs.{FileSystem, Path}

object ActiveUserDataFrameReport extends TwitterReport {
  val log = getLogger("ActiveUserDataFrameReport")

  def main(args: Array[String]): Unit = {
    val spark = setSparkSession("TwitterActiveUserDataFrameReport")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (fs.exists(new Path(dataPath))) {
      log.debug("=======> Building Twitter ActiveUser Report for date " + today)
      val inputDataForReport = getInputDataForReport(dataPath, spark)

      log.debug("=======> Stage 1: Twitter Data Read Complete !")
      log.debug("=======> Stage 2: Generating Report for Active Users..")
      val report = execute(getReportWithDataFrameProcessing, spark, inputDataForReport)

      log.debug("=======> Stage 3: Saving Report on HDFS...")
      saveReportToHdfs(report, reportSavePath + "/dataFrameReport")
      spark.stop
    }
    else {
      log.error("Data for Report Creation is not available !")
      spark.stop
      System.exit(1)
    }
  }
}
