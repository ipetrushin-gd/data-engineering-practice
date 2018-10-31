package com.gd.twitteranalytics

import com.gd.twitteranalytics.reports.TwitterReportWriter
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{current_date, lit}
import org.scalatest.{BeforeAndAfter, FreeSpec}

class TwitterReportWriterTest extends FreeSpec with DataFrameSuiteBase with BeforeAndAfter{

  val dataPathForReport = getClass.getResource("/expectedResultFromReport.csv").getPath
  val reportSavePath = "twitter-analytics/reporting/testFolder"

  after {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(reportSavePath),true)
  }

  "TwitterReportWriter " - {
    "saveReportToHdfs" - {
      "should save the report at given path in FileSystem" in {
        val expectedDf = spark.read.option("header",true)
          .option("inferSchema",true)
          .csv(dataPathForReport).withColumn("Date",lit(current_date))

        TwitterReportWriter.saveReportToHdfs(expectedDf,reportSavePath)
        val fs = FileSystem.get(sc.hadoopConfiguration)
        assert (fs.exists(new Path(reportSavePath)) === true)
      }
    }
  }
}
