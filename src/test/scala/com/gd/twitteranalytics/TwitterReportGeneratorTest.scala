package com.gd.twitteranalytics

import com.holdenkarau.spark.testing.{DataFrameSuiteBase}
import org.scalatest.{BeforeAndAfter, FreeSpec}
import org.apache.spark.sql.functions._

class TwitterReportGeneratorTest extends FreeSpec with BeforeAndAfter with DataFrameSuiteBase {

  val inputDataPath = getClass.getResource("/dataInputForReport.csv").getPath
  val expectedDataPath = getClass.getResource("/expectedResultFromReport.csv").getPath

  "TwitterReportGenerator " - {
    "getReportWithDataFrameProcessing" - {
      "should generate report with users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header",true).option("inferSchema",true).csv(inputDataPath)
        val expectedDf = spark.read.option("header",true)
                                   .option("inferSchema",true)
                                   .csv(expectedDataPath).withColumn("Date",lit(current_date))

        val actualDf = TwitterReportGenerator.getReportWithSqlProcessing(spark,inputDf)
        assert(expectedDf.except(actualDf).rdd.isEmpty === true)
      }
    }
    "getReportWithSqlProcessing" - {
      "should generate report with users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header",true).option("inferSchema",true).csv(inputDataPath)
        val expectedDf = spark.read.option("header",true)
          .option("inferSchema",true)
          .csv(expectedDataPath).withColumn("Date",lit(current_date))

        val actualDf = TwitterReportGenerator.getReportWithDataFrameProcessing(spark,inputDf)
        assert(expectedDf.except(actualDf).rdd.isEmpty === true)
      }
    }
    "getReportWithDataSetProcessing" - {
      "should generate report with users that are occuring more than 10 times per location" in {
        val inputDf = spark.read.option("header",true).option("inferSchema",true).csv(inputDataPath)
        val expectedDf = spark.read.option("header",true)
          .option("inferSchema",true)
          .csv(expectedDataPath).withColumn("Date",lit(current_date))

        val actualDf = TwitterReportGenerator.getReportWithDataSetProcessing(spark,inputDf)
        assert(expectedDf.except(actualDf).rdd.isEmpty === true)
      }
    }
  }
}
