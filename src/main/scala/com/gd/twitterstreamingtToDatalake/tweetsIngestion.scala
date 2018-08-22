package com.gd.twitterstreamingtToDatalake

import com.gd.twitterstreamingtToDatalake.StreamingTweetsJob.ssc
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

object tweetsIngestion {

  def getTweets: DStream[Status] = {

    val filters: Seq[String] = Seq("KerlaFloods")

    val tweets = TwitterUtils.createStream(ssc, None, filters)

    val englishTweets = tweets.filter(_.getLang() == "en")

    englishTweets

  }

}
