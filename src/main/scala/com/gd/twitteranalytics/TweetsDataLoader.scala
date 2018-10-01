package com.gd.twitteranalytics

import org.apache.spark.sql.DataFrame

object TweetsDataLoader {

  def saveOutputToHdfs(inputDataFrame:DataFrame,savePath:String,partitionCol:String)={
    inputDataFrame.write.partitionBy(partitionCol).
      format("parquet").mode("append").save(savePath)
  }
}