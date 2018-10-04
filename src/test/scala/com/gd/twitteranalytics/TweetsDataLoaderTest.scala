package com.gd.twitteranalytics

import java.io.File
import com.gd.twitteranalytics.util.AppConfigReader
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FreeSpec

class TweetsDataLoaderTest extends FreeSpec with DataFrameSuiteBase {

  "Methods tests" - {
    "saveOutputToHdfs when presented with dataframe, savePath and partitionColumn" - {
      "should create event_datetime folder structure in fileSystem at given path" in {
        val text = "This is the #Sample #text for #test purpose"
        val event_date = "20180928"
        val savePath = AppConfigReader.getAppConfigurables(0)
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val sourceDf = Seq((event_date,text),(event_date,text)).toDF("event_date","payload")
        TweetsDataLoader.saveOutputToHdfs(sourceDf,savePath)
        val file = new File(savePath+"/event_date="+s"${event_date}")
        assert(file.exists ===true)
      }
    }
  }
}