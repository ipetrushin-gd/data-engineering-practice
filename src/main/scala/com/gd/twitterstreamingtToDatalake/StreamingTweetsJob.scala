package com.gd.twitterstreamingtToDatalake

import org.apache.log4j.Logger

object StreamingTweetsJob extends SparkSessionWrapper {

  //Set the Logger Level
  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {

    if (validateConfiguration(args)) {
      System.exit(1)
    }
    val arr = args.take(4)
    TweetsIngestion.configureTwitter(arr)

    //Incase Only tweets related to a specific Hashtags are needed..
    val filters = args.takeRight(args.length - 4)

    val englishTweets = TweetsIngestion.getTweets(filters)
    val text = TransformTweets.getText(englishTweets)
    val hashTags = TransformTweets.getHashTags(text)

    hashTags.saveAsTextFiles("tweets", "json")

    ssc.start()
    ssc.awaitTermination()
  }

  def validateConfiguration(args: Array[String]): Boolean = {

    if (args.length < 4) {
      log.error("Please login with Twitter Keys !")
      log.error("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      true
    }
    else false
  }
}