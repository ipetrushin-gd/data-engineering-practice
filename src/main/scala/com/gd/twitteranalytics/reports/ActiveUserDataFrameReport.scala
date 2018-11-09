package com.gd.twitteranalytics.reports

import ActiveUserReportProcessor._
import TwitterReportGenerator._

object ActiveUserDataFrameReport extends TwitterReport {
  val log = getLogger("ActiveUserDataFrameReport")

  def main(args: Array[String]): Unit = {
    val spark = setSparkSession("TwitterActiveUserDataFrameReport")
    if (validateReportPath(spark,log)){
      log.debug("=======> Stage 1: Reading Data for Twitter Active User Report...")
      val inputDataForReport = getInputDataForActiveUserReport(dataPath, spark,log)

      log.debug("=======> Stage 2: Creating report for Twitter Active Users...")
      val report = execute(getReportWithDataFrameProcessing, spark, inputDataForReport, reportDate)

      log.debug("=======> Stage 3: Saving Report on HDFS...")
      saveActiveUserReport(report,"/dataFrameReport",log)

      spark.stop
    }
    else {
      stopJob(spark,log)
    }
  }
}
