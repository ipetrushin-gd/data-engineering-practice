package com.gd.twitteranalytics

import org.apache.spark.sql.DataFrame

object TwitterReportWriter {

  def saveReportToHdfs(inputDataSet: DataFrame, savePath:String)={
    inputDataSet.write.partitionBy("Date").
      format("csv").mode("append").save(savePath)
  }
}