package com.gd.twitterstreaming

import com.gd.twitterstreaming.util.TweetsConstant
import org.apache.spark.sql.DataFrame

object TweetsDataSave {

  def saveOutputToHdfs(inputDataFrame:DataFrame)={

    inputDataFrame.write.partitionBy(TweetsConstant.PARTITION_COLUMN).
      format("csv").mode("append").option("path",TweetsConstant.SAVEPATH).
      saveAsTable(TweetsConstant.TABLENAME)
    }
  }