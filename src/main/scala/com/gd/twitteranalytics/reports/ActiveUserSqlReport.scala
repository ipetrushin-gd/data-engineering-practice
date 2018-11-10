package com.gd.twitteranalytics.reports

import com.gd.twitteranalytics.reports.ActiveUserReportBuildingStrategy.buildWithSqlAPI
import com.gd.twitteranalytics.reports.ReportType.ReportType

object ActiveUserSqlReport extends ActiveUsersReport {

  override def getReportName(): String = "ActiveUserSqlReport"

  override def getReportType(): ReportType = ReportType.PlanSql

  override def getReportBuildingStrategy() = buildWithSqlAPI

  def main(args: Array[String]): Unit = {
    run(getSparkSession())
  }

}
