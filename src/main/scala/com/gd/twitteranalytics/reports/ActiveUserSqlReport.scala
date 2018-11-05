package com.gd.twitteranalytics.reports

import ActiveUserReportProcessor._
import TwitterReportGenerator._

object ActiveUserSqlReport extends TwitterReport {
  val log = getLogger("ActiveUserSqlReport")

  def main(args: Array[String]): Unit = {
    val spark = setSparkSession("TwitterActiveUserSqlReport")
    if (validateReportPath(spark,log)){
      val inputDataForReport = getInputDataForActiveUserReport(dataPath, spark,log)
      val report = execute(getReportWithSqlProcessing, spark, inputDataForReport,reportDate)
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
