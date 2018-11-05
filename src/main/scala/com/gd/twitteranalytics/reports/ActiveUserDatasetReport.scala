package com.gd.twitteranalytics.reports

import ActiveUserReportProcessor._
import TwitterReportGenerator._

object ActiveUserDatasetReport extends TwitterReport{
  val log = getLogger("ActiveUserDataSetReport")

  def main(args: Array[String]): Unit = {
    val spark = setSparkSession("TwitterActiveUserDataSetReport")
    if (validateReportPath(spark,log)){
      val inputDataForReport = getInputDataForActiveUserReport(dataPath, spark,log)
      val report = execute(getReportWithDataSetProcessing, spark, inputDataForReport, reportDate)
      saveActiveUserReport(report,"/dataSetReport",log)
      spark.stop
    }
    else {
      printErrorLogs(log)
      spark.stop
      System.exit(1)
    }
  }
}