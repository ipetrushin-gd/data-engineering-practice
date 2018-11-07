package com.gd.twitteranalytics.reports

import ActiveUserReportProcessor._
import TwitterReportGenerator._

object ActiveUserSqlReport extends TwitterReport {
  val log = getLogger("ActiveUserSqlReport")

  def main(args: Array[String]): Unit = {
    val spark = setSparkSession("TwitterActiveUserSqlReport")
    if (validateReportPath(spark,log)){
      log.debug("=======> Stage 1: Reading Data for Twitter Active User Report...")
      val inputDataForReport = getInputDataForActiveUserReport(dataPath, spark,log)

      log.debug("=======> Stage 1: Creating report for Twitter Active Users...")
      val report = execute(getReportWithSqlProcessing, spark, inputDataForReport,reportDate)

      log.debug("=======> Stage 3: Saving Report on HDFS...")
      saveActiveUserReport(report,"/sqlReport",log)

      spark.stop
    }
    else {
      printErrorLogs(log)
      spark.stop
      System.exit(1)
    }
  }
}
