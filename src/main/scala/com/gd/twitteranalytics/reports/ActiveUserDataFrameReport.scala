package com.gd.twitteranalytics.reports

import ActiveUserReportProcessor._
import TwitterReportGenerator.execute

object ActiveUserDataFrameReport extends TwitterReport {
  val log = getLogger("ActiveUserDataFrameReport")

  def main(args: Array[String]): Unit = {
    val spark = setSparkSession("TwitterActiveUserDataFrameReport")
    if (validateReportPath(spark,log)){
      val inputDataForReport = getInputDataForActiveUserReport(dataPath, spark,log)
      val report = execute(getReportWithDataFrameProcessing, spark, inputDataForReport, reportDate)
      saveActiveUserReport(report,"/dataFrameReport",log)
      spark.stop
    }
    else {
      printErrorLogs(log)
      spark.stop
      System.exit(1)
    }
  }
}
