package com.gd.twitterstreamingtToDatalake

import com.gd.twitterstreamingtToDatalake.StreamingTweetsJob.ssc
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status


object TweetsIngestion {

  def getTweets(arr:Array[String]): DStream[Status] = {

    System.setProperty("twitter4j.oauth.consumerKey", arr(0))
    System.setProperty("twitter4j.oauth.consumerSecret", arr(1))
    System.setProperty("twitter4j.oauth.accessToken", arr(2))
    System.setProperty("twitter4j.oauth.accessTokenSecret", arr(3))

    //val filters: Seq[String] = Seq("narendramodi")

    val tweets = TwitterUtils.createStream(ssc, None)//, filters)

    val englishTweets = tweets.filter(_.getLang() == "en")

    englishTweets

  }

}
