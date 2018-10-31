package com.gd.twitteranalytics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.functions.{current_date, lit}
import org.scalatest.{BeforeAndAfter, FreeSpec}

class ReportProcessorTest extends FreeSpec with BeforeAndAfter with DataFrameSuiteBase {

  val inputDataPath = getClass.getResource("/dataInputForReport.csv").getPath
  val expectedDataPath = getClass.getResource("/expectedResultFromReport.csv").getPath

  "ReportProcessor " - {
    "getReportWithDataFrameProcessing" - {
      "should generate report with maximum of 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header",true).option("inferSchema",true).csv(inputDataPath)
        val expectedDf = spark.read.option("header",true)
          .option("inferSchema",true)
          .csv(expectedDataPath).withColumn("Date",lit(current_date))

        val actualDf = ReportProcessor.getReportWithDataFrameProcessing(spark,inputDf)
        assert(expectedDf.except(actualDf).rdd.isEmpty === true)
      }
    }
    "getReportWithSqlProcessing" - {
      "should generate report with maximum of 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header",true).option("inferSchema",true).csv(inputDataPath)
        val expectedDf = spark.read.option("header",true)
          .option("inferSchema",true)
          .csv(expectedDataPath).withColumn("Date",lit(current_date))

        val actualDf = ReportProcessor.getReportWithSqlProcessing(spark,inputDf)
        assert(expectedDf.except(actualDf).rdd.isEmpty === true)
      }
    }
    "getReportWithDataSetProcessing" - {
      "should generate report with maximum of 5 users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header",true).option("inferSchema",true).csv(inputDataPath)
        val expectedDf = spark.read.option("header",true)
          .option("inferSchema",true)
          .csv(expectedDataPath).withColumn("Date",lit(current_date))

        val actualDf = ReportProcessor.getReportWithDataSetProcessing(spark,inputDf)
        assert(expectedDf.except(actualDf).rdd.isEmpty === true)
      }
    }
  }
}

