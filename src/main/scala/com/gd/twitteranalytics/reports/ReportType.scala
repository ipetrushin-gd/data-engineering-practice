package com.gd.twitteranalytics.reports

object ReportType extends Enumeration {
  type ReportType = Value
  val Dataframe, Dataset, PlanSql = Value
}
