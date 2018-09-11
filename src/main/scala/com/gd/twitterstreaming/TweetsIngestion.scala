package com.gd.twitterstreaming

import org.apache.spark.streaming.twitter.TwitterUtils

object TweetsIngestion extends SparkSessionWrapper {

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