package com.gd.twitteranalytics.reports

import ActiveUserReportProcessor._
import TwitterReportGenerator._
import TwitterReportWriter._
import org.apache.hadoop.fs.{FileSystem, Path}

object ActiveUserDatasetReport extends TwitterReport{
  val log = getLogger("ActiveUserDataSetReport")

  def main(args: Array[String]): Unit = {
    val spark = setSparkSession("TwitterActiveUserDataSetReport")
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    if (fs.exists(new Path(dataPath))) {
      log.debug("=======> Building Twitter ActiveUser Report for date " + today)
      val inputDataForReport = getInputDataForReport(dataPath, spark)

      log.debug("=======> Stage 1: Twitter Data Read Complete !")
      log.debug("=======> Stage 2: Generating Report for Active Users..")
      val report = execute(getReportWithDataSetProcessing, spark, inputDataForReport)

      log.debug("=======> Stage 3: Saving Report on HDFS...")
      saveReportToHdfs(report, activeUserReportSavePath + "/DataSetReport")
      spark.stop
    }
    else {
      log.error("Data for Report Creation is not available !")
      spark.stop
      System.exit(1)
    }
  }
}