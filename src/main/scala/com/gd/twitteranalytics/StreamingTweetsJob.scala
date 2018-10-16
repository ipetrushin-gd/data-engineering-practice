package com.gd.twitteranalytics

import com.gd.twitteranalytics.TweetsTransformer._
import com.gd.twitteranalytics.TweetsDataLoader._
import com.gd.twitteranalytics.util.AppConfigReader
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{Date => JavaDate}

object StreamingTweetsJob {

  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {
      val authKeys = AppConfigReader.getTwitterAuthKeys
      if (ConfigValidator.isConfValid(authKeys)) {
        val Array(savePath, tweetsLangFilter, hashTagsFilter) = AppConfigReader.getAppConfigurables
        val dateColumn = "event_date"
        val ssc = setSparkStreamingContext

        TweetsDataReader.configureTwitter(authKeys)

        val englishTweets = TweetsDataReader.getTweets(ssc, Array(hashTagsFilter), tweetsLangFilter)
        val tweetsInfo = getCreationDateAndStatus(englishTweets)
        processTweetsInfo(tweetsInfo, savePath, dateColumn)

        ssc.start
        ssc.awaitTermination()
      }
      else
        System.exit(1)
  }
// TODO: Configure CheckPointing
  def setSparkStreamingContext():StreamingContext = {
    val sparkConf = new SparkConf().
                        setAppName("TwitterStreamingPipeLine")
    new StreamingContext(sparkConf, Seconds(2))
  }

  def processTweetsInfo(tweetsInfo: DStream[(JavaDate, String)],path:String,dateColumn:String) = {
    tweetsInfo.foreachRDD { rdd =>
      var tweetsDataFrame = convertRddToDataFrame(rdd)
      tweetsDataFrame =  updateDateColFormat(tweetsDataFrame,dateColumn)
      val tweetsDataSet = convertDataFrameToDataSet(tweetsDataFrame)
      saveOutputToHdfs(tweetsDataSet,path)
    }
  }
}