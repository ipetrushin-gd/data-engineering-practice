package com.gd.twitteranalytics

import org.apache.spark.sql.Dataset

object TweetsDataLoader {

  def saveOutputToHdfs(inputDataSet: Dataset[Tweet],savePath:String)={
    inputDataSet.write.partitionBy("event_date").
      format("parquet").mode("append").save(savePath)
  }
}