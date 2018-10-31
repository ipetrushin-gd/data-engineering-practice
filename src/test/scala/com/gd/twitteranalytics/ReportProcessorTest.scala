package com.gd.twitteranalytics

import com.gd.twitteranalytics.reports.ReportProcessor._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.{current_date, lit}
import org.scalatest.{BeforeAndAfter, FreeSpec}

class ReportProcessorTest extends FreeSpec with BeforeAndAfter with DataFrameSuiteBase {

  val inputDataPath = getClass.getResource("/dataInputForReport.csv").getPath
  val expectedDataPath = getClass.getResource("/expectedResultFromReport.csv").getPath

  "ReportProcessor " - {
    "getReportWithDataFrameProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header", true).option("inferSchema", true).csv(inputDataPath)
        val expectedDf = spark.read.option("header", true)
          .option("inferSchema", true)
          .csv(expectedDataPath).withColumn("Date", lit(current_date))

        val actualDf = getReportWithDataFrameProcessing(spark, inputDf)
        assert(actualDf.except(expectedDf).rdd.isEmpty === true)
      }
    }
    "getReportWithSqlProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header", true).option("inferSchema", true).csv(inputDataPath)
        val expectedDf = spark.read.option("header", true)
          .option("inferSchema", true)
          .csv(expectedDataPath).withColumn("Date", lit(current_date))

        val actualDf = getReportWithSqlProcessing(spark, inputDf)
        assert(actualDf.except(expectedDf).rdd.isEmpty === true)
      }
    }
    "getReportWithDataSetProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header", true).option("inferSchema", true).csv(inputDataPath)
        val expectedDf = spark.read.option("header", true)
          .option("inferSchema", true)
          .csv(expectedDataPath).withColumn("Date", lit(current_date))

        val actualDf = getReportWithDataSetProcessing(spark, inputDf)
        assert(actualDf.except(expectedDf).rdd.isEmpty === true)
      }
    }
  }
}

