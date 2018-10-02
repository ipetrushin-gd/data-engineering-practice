package com.gd.twitteranalytics

import com.gd.twitteranalytics.TweetsTransformer._
import com.gd.twitteranalytics.TweetsDataLoader._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{Date => JavaDate}
import com.gd.twitteranalytics.util.AppConfigReader

object StreamingTweetsJob {

  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {
      val authKeys = ConfigValidator.getTwitterAccessKeys
      if (ConfigValidator.isConfValid(authKeys)) {
        //Incase Only tweets related to a specific Hashtags are needed..
        val filters = AppConfigReader.HashTags

        val dateColumn = "event_date"
        val ssc = setSparkStreamingContext

        TweetsDataReader.configureTwitter
        val englishTweets = TweetsDataReader.getTweets(ssc, Array(filters))
        val tweetsInfo = getDateAndText(englishTweets)

        processTweetsInfo(tweetsInfo,AppConfigReader.SavePath,dateColumn)

        ssc.start
        ssc.awaitTermination()
      }
      else
        System.exit(1)
  }

  def setSparkStreamingContext() = {
    val sparkConf = new SparkConf().
                        setAppName("twitter-analytics").setMaster(AppConfigReader.MasterUrl)
    new StreamingContext(sparkConf, Seconds(2))
  }

  def processTweetsInfo(tweetsInfo: DStream[(JavaDate, String)],path:String,dateColumn:String) = {
    tweetsInfo.foreachRDD { rdd =>
      var tweetsDataFrame = convertRddToDataFrame(rdd)
      tweetsDataFrame = updateDateColFormat(tweetsDataFrame,dateColumn)
      saveOutputToHdfs(tweetsDataFrame,path,dateColumn)
    }
  }
}