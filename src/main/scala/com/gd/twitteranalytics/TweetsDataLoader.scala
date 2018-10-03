package com.gd.twitteranalytics

import org.apache.spark.sql.DataFrame

object TweetsDataLoader {

  def saveOutputToHdfs(inputDataFrame:DataFrame,savePath:String)={
    inputDataFrame.write.partitionBy("event_date").
      format("parquet").mode("append").save(savePath)
  }
}