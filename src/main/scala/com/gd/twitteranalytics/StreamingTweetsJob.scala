package com.gd.twitteranalytics

import com.gd.twitteranalytics.TransformTweets._
import com.gd.twitteranalytics.TweetsDataSave._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{Date => JavaDate}

import com.gd.twitteranalytics.util.ReadTwitterConf
import com.typesafe.config.ConfigException

object StreamingTweetsJob {

  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {
    try {
      val authKeys = ConfigValidator.validateTwitterAuth

      if (ConfigValidator.isConfValid(authKeys)) {
        TweetsIngestion.configureTwitter()

        //Incase Only tweets related to a specific Hashtags are needed..
        val filters = ReadTwitterConf.HASHTAGS

        val ssc = setSparkSessionConf
        val englishTweets = TweetsIngestion.getTweets(ssc, Array(filters))
        val tweetsInfo = getDateAndText(englishTweets)
        processTweetsInfo(tweetsInfo)

        ssc.start
        ssc.awaitTermination()
      }
      else
        System.exit(1)
    }
    catch {
      case exception: Exception => {
        handleException(exception)
      }
    }
  }

  def setSparkSessionConf() = {

    //TODO: Add Spark Configuration via command line
    val sparkConf = new SparkConf().
      setAppName(ReadTwitterConf.APPNAME).setMaster(ReadTwitterConf.MASTERURL)
    new StreamingContext(sparkConf, Seconds(2))
  }

  def processTweetsInfo(tweetsInfo: DStream[(JavaDate, String)]) = {
    tweetsInfo.foreachRDD { rdd =>
      var tweetsDataFrame = convertRddToDataFrame(rdd)
      tweetsDataFrame = updateDateColFormat(tweetsDataFrame)
      saveOutputToHdfs(tweetsDataFrame)
    }
  }

  def handleException(exception: Exception) {
    exception match {
      case exception: ConfigException.Missing =>
        if (exception.getMessage == null)
          log.error("Please check the Authentication Twitter Keys !")
        else
          log.error(exception.getMessage)
      case e: Exception => log.error(e.printStackTrace)
    }
  }
}