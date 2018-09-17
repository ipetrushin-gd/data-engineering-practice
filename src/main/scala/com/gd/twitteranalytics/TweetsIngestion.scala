package com.gd.twitteranalytics

import com.gd.twitteranalytics.util.TweetsConstant
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext

object TweetsIngestion {

  def configureTwitter(authKeys: Array[String]) {

    System.setProperty("twitter4j.oauth.consumerKey", authKeys(0))
    System.setProperty("twitter4j.oauth.consumerSecret", authKeys(1))
    System.setProperty("twitter4j.oauth.accessToken", authKeys(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", authKeys(3))
  }

  def getTweets(ssc:StreamingContext,filters:Array[String]) = {

    val tweets = TwitterUtils.createStream(ssc, None, filters)
    tweets.filter(_.getLang == TweetsConstant.TWEETS_LANG_FILTER)
  }
}