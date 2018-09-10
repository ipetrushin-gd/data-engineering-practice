package com.gd.twitterstreamingtToDatalake

import com.gd.twitterstreamingtToDatalake.StreamingTweetsJob.ssc
import org.apache.spark.streaming.twitter.TwitterUtils

object TweetsIngestion {

  def configureTwitter(authKeys: Array[String]) {

    System.setProperty("twitter4j.oauth.consumerKey", authKeys(0))
    System.setProperty("twitter4j.oauth.consumerSecret", authKeys(1))
    System.setProperty("twitter4j.oauth.accessToken", authKeys(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", authKeys(3))
  }

  def getTweets(filters:Array[String]) = {

    val tweets = TwitterUtils.createStream(ssc, None, filters)
    val englishTweets = tweets.filter(_.getLang == "en")

    englishTweets
  }
}
