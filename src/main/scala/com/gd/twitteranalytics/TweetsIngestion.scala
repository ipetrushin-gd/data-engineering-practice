package com.gd.twitteranalytics

import com.gd.twitteranalytics.util.ReadTwitterConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext

object TweetsIngestion {

  def configureTwitter() {
    System.setProperty("twitter4j.oauth.consumerKey", ReadTwitterConf.CONSUMER_KEY)
    System.setProperty("twitter4j.oauth.consumerSecret", ReadTwitterConf.CONSUMER_SECRET)
    System.setProperty("twitter4j.oauth.accessToken", ReadTwitterConf.ACCESS_TOKEN)
    System.setProperty("twitter4j.oauth.accessTokenSecret", ReadTwitterConf.ACCESS_SECRET)
  }

  def getTweets(ssc:StreamingContext,filters:Array[String]) = {

    val tweets = TwitterUtils.createStream(ssc, None, filters)
    tweets.filter(_.getLang == ReadTwitterConf.TWEETS_LANG_FILTER)
  }
}