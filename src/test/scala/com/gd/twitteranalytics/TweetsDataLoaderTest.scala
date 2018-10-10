package com.gd.twitteranalytics

import java.util.{Date => JavaDate}
import java.sql.Date

import com.gd.twitteranalytics.util.AppConfigReader
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FreeSpec}

class TweetsDataLoaderTest extends FreeSpec with DataFrameSuiteBase with BeforeAndAfter{

    val payload = "This is the #Sample #text for #test purpose"
    val event_date = new Date(new JavaDate("Wed Sep 26 10:37:20 PDT 2018").getTime)
    val sampleDate = "2018-09-26"
    val savePath = AppConfigReader.getAppConfigurables(0)+"/event_date="+s"${sampleDate}"

  after {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(savePath),true)
  }

  "TweetsDataLoader" - {
    "saveOutputToHdfs function when presented with Dataset[Tweet] and savePath" - {
      "should create event_date=YYYY-MM-DD folder structure in fileSystem at given path" in {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val tweetDataSet = Seq(Tweet(event_date,payload),Tweet(event_date,payload)).toDS
        TweetsDataLoader.saveOutputToHdfs(tweetDataSet,savePath)
        val fs = FileSystem.get(sc.hadoopConfiguration)
        assert (fs.exists(new Path(savePath)) === true)
      }
    }
    "saveOutputToHdfs function when saves data on HDFS at savepath" - {
      "it should be in parquet format and data content should be same as received" in {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val tweetDataSet = Seq(Tweet(event_date,payload),Tweet(event_date,payload)).toDS
        TweetsDataLoader.saveOutputToHdfs(tweetDataSet,savePath)
        val dataFromHdfsFile = spark.read.parquet(savePath)
        assert(dataFromHdfsFile.select("payload").first().mkString === payload)
      }
    }
  }
}