package com.gd.twitteranalytics

import org.apache.spark.sql.DataFrame
import com.gd.twitteranalytics.util.ReadTwitterConf

object TweetsDataSave {

  def saveOutputToHdfs(inputDataFrame:DataFrame)={

    inputDataFrame.write.partitionBy(ReadTwitterConf.PARTITION_COLUMN).
      format("parquet").mode("append").save(ReadTwitterConf.SAVEPATH)
    }
  }