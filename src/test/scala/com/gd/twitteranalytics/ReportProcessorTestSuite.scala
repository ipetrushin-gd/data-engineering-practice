package com.gd.twitteranalytics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.{current_date, lit}
import org.scalatest.FreeSpec

trait ReportProcessorTestSuite extends FreeSpec with DataFrameSuiteBase{

  val inputDataPath = getClass.getResource("/dataInputForReport.csv").getPath
  val expectedDataPath = getClass.getResource("/expectedResultFromReport.csv").getPath

  val inputDfForActiveUserReport = spark.read.option("header", true).option("inferSchema", true).csv(inputDataPath)
  val expectedDfForActiveUserReport = spark.read.option("header", true)
    .option("inferSchema", true)
    .csv(expectedDataPath).withColumn("date", lit(current_date))
}
