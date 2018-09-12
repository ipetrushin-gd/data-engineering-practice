package com.gd.twitterstreaming

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingTweetsJob {

  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {

    val ssc = setSparkSessionConf
    if (ConfigValidator.isConfValid(args)) {
      val authorizationKeys = args.take(4)
      TweetsIngestion.configureTwitter(authorizationKeys)

      //Incase Only tweets related to a specific Hashtags are needed..
      val filters = args.takeRight(args.length - 4)

      val englishTweets = TweetsIngestion.getTweets(ssc,filters)
      val tweetInfo = TransformTweets.getText(englishTweets)
      val hashTags = TransformTweets.getHashTags(tweetInfo)

      hashTags.saveAsTextFiles("tweets", "json")
      ssc.start
      ssc.awaitTermination()
    }
    else
      System.exit(1)
  }

  def setSparkSessionConf() = {

    //TODO: Add Spark Configuration via command line
    val conf = new SparkConf()
    conf.setAppName("twitterStreaming").setMaster("local")
    new StreamingContext(conf, Seconds(5))
  }
}