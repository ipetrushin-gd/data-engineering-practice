package com.gd.twitteranalytics

import com.gd.twitteranalytics.util.AppConfigReader
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext

object TweetsDataReader {

  def configureTwitter() {
    System.setProperty("twitter4j.oauth.consumerKey", AppConfigReader.ConsumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", AppConfigReader.ConsumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", AppConfigReader.AccessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", AppConfigReader.AccessSecret)
  }

  def getTweets(ssc:StreamingContext,filters:Array[String]) = {
    val tweets = TwitterUtils.createStream(ssc, None, filters)
    tweets.filter(_.getLang == AppConfigReader.TweetslangFilter)
  }
}