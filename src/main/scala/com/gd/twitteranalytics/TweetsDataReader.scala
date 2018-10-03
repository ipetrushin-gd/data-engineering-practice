package com.gd.twitteranalytics

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TweetsDataReader {

  def configureTwitter(twitterAuthKeys: Array[String]) {
    System.setProperty("twitter4j.oauth.consumerKey", twitterAuthKeys(0))
    System.setProperty("twitter4j.oauth.consumerSecret", twitterAuthKeys(1))
    System.setProperty("twitter4j.oauth.accessToken", twitterAuthKeys(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", twitterAuthKeys(3))
  }

  def getTweets(ssc:StreamingContext,filters:Array[String],langFilter:String) : DStream[Status] = {
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    tweets.filter(_.getLang == langFilter)
  }
}