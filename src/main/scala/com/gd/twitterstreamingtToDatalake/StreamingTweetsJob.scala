package com.gd.twitterstreamingtToDatalake

import org.apache.log4j.{Level, Logger}

object StreamingTweetsJob extends SparkSessionWrapper {

  //Set the Logger Level
  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {

    if (validateConfiguraiton(args)) {
      System.exit(1)
    }

    val arr = args.take(4)

    TweetsIngestion.configureTwitter(arr)

    val englishTweets = TweetsIngestion.getTweets

    val hashTags = TransformTweets.getText(englishTweets)

    hashTags.saveAsTextFiles("tweets", "json")

    ssc.start()
    ssc.awaitTermination()

  }

  def validateConfiguraiton(args: Array[String]): Boolean = {

    if (args.length < 4) {
      log.error("Please login with Twitter Keys !")
      System.err.println("Usage: TwitterData <ConsumerKey><ConsumerSecret><accessToken><accessTokenSecret>" +
        "[<filters>]")
      true
    }
    else false

  }
}
