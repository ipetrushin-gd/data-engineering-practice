package com.gd.twitteranalytics.reports

import com.gd.twitteranalytics.reports.ActiveUserReportBuildingStrategy.buildWithDataFrameAPI
import com.gd.twitteranalytics.reports.ReportType.ReportType

object ActiveUserDataFrameReport extends ActiveUsersReport {
  override def getReportName(): String = "ActiveUserDataframeReport"

  override def getReportType(): ReportType = ReportType.Dataframe

  override def getReportBuildingStrategy() = buildWithDataFrameAPI

  def main(args: Array[String]): Unit = {
    run(getSparkSession())
  }
}
