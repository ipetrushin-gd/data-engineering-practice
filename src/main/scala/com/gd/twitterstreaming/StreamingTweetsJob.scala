package com.gd.twitterstreaming

import org.apache.log4j.Logger

object StreamingTweetsJob extends SparkSessionWrapper {

  val log = Logger.getLogger(StreamingTweetsJob.getClass.getName)

  def main(args: Array[String]): Unit = {

    if (ConfigValidator.isConfValid(args)) {
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
}