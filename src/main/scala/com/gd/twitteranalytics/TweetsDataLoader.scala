package com.gd.twitteranalytics

import org.apache.spark.sql.{DataFrame, Dataset}

object TweetsDataLoader {

  def saveOutputToHdfs(inputDataSet: Dataset[Tweet],savePath:String)={
    inputDataSet.write.partitionBy("event_date").
      format("parquet").mode("append").save(savePath)
  }

  def saveReportToHdfs(inputDataSet: DataFrame, savePath:String)={
    inputDataSet.write.partitionBy("Date").
      format("csv").mode("append").save(savePath)
  }
}