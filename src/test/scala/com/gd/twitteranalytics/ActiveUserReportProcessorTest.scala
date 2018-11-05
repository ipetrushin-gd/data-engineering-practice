package com.gd.twitteranalytics

import com.gd.twitteranalytics.reports.ActiveUserReportProcessor._
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.lit
import org.scalatest.{BeforeAndAfter, FreeSpec}

class ActiveUserReportProcessorTest extends FreeSpec with BeforeAndAfter with DataFrameSuiteBase {

  val inputDataPath = getClass.getResource("/dataInputForReport.csv").getPath
  val expectedDataPath = getClass.getResource("/expectedResultFromReport.csv").getPath
  val reportDate = "2018-09-20"

  "ActiveUserReportProcessor " - {
    "getReportWithDataFrameProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header", true).option("inferSchema", true).csv(inputDataPath)
        val expectedDf = spark.read.option("header", true)
          .option("inferSchema", true)
          .csv(expectedDataPath).withColumn("date", lit(reportDate))
        val actualDf = getReportWithDataFrameProcessing(spark, inputDf,reportDate)

        assert(actualDf.except(expectedDf).rdd.isEmpty === true)
      }
    }
    "getReportWithSqlProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header", true).option("inferSchema", true).csv(inputDataPath)
        val expectedDf = spark.read.option("header", true)
          .option("inferSchema", true)
          .csv(expectedDataPath).withColumn("date", lit(reportDate))
        val actualDf = getReportWithSqlProcessing(spark, inputDf,reportDate)

        assert(actualDf.except(expectedDf).rdd.isEmpty === true)
      }
    }
    "getReportWithDataSetProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header", true).option("inferSchema", true).csv(inputDataPath)
        val expectedDf = spark.read.option("header", true)
          .option("inferSchema", true)
          .csv(expectedDataPath).withColumn("date", lit(reportDate))
        val actualDf = getReportWithDataSetProcessing(spark, inputDf,reportDate)

        assert(actualDf.except(expectedDf).rdd.isEmpty === true)
      }
    }
  }
}

