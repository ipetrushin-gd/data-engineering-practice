package com.gd.twitterstreamingtToDatalake

import org.apache.log4j.Logger

object StreamingTweetsJob extends SparkSessionWrapper {

  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {

    if (isConfValid(args)) {
      val authorizationKeys = args.take(4)
      TweetsIngestion.configureTwitter(authorizationKeys)

      //Incase Only tweets related to a specific Hashtags are needed..
      val filters = args.takeRight(args.length - 4)

      val englishTweets = TweetsIngestion.getTweets(filters)
      val text = TransformTweets.getText(englishTweets)
      val hashTags = TransformTweets.getHashTags(text)

      hashTags.saveAsTextFiles("tweets", "json")

      ssc.start()
      ssc.awaitTermination()
    }
    else
      System.exit(1)
  }

  def isConfValid(keys: Array[String]): Boolean = {

    if (keys.length < 4) {
      log.error("Number of keys should be at least four! Please login with all keys !")
      false
    }
    else if (validateTwitterKeys(keys)) true
    else {
      log.error("Twitter keys are not correct !")
      log.error("Usage: TwitterData <ConsumerKey> <ConsumerSecret> <accessToken> <accessTokenSecret> " +
        "[<filters (If Any)>]")
      false
    }
  }

    def validateTwitterKeys(keys:Array[String]):Boolean = {
      keys.forall(_.contains(""))
    }
}