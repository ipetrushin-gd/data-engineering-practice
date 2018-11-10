package com.gd.twitteranalytics.reports

import com.gd.twitteranalytics.reports.ActiveUserReportBuildingStrategy.buildWithDatasetAPI
import com.gd.twitteranalytics.reports.ReportType.ReportType

object ActiveUserDatasetReport extends ActiveUsersReport {
  override def getReportName(): String = "ActiveUserDatasetReport"

  override def getReportType(): ReportType = ReportType.Dataset

  override def getReportBuildingStrategy() = buildWithDatasetAPI

  def main(args: Array[String]): Unit = {
    run(getSparkSession())
  }
}
