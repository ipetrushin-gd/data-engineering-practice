package com.gd.twitteranalytics

import com.gd.twitteranalytics.reports.ActiveUserReportProcessor._

class ActiveUserReportProcessorTest extends ReportProcessorTestSuite {

  "ActiveUserReportProcessor " - {
    "getReportWithDataFrameProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val actualDf = getReportWithDataFrameProcessing(spark, inputDfForActiveUserReport)
        assert(actualDf.except(expectedDfForActiveUserReport).rdd.isEmpty === true)
      }
    }
    "getReportWithSqlProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val actualDf = getReportWithSqlProcessing(spark, inputDfForActiveUserReport)
        assert(actualDf.except(expectedDfForActiveUserReport).rdd.isEmpty === true)
      }
    }
    "getReportWithDataSetProcessing" - {
      "should generate report with max of Top 5 users that are occuring more than 10 times per location" in {
        val actualDf = getReportWithDataSetProcessing(spark, inputDfForActiveUserReport)
        assert(actualDf.except(expectedDfForActiveUserReport).rdd.isEmpty === true)
      }
    }
  }
}