package com.gd.twitterstreamingtToDatalake

import com.gd.twitterstreamingtToDatalake.StreamingTweetsJob.ssc
import org.apache.spark.streaming.twitter.TwitterUtils

object tweetsIngestion {

  def getTweets = {

    val filters: Seq[String] = Seq("narendramodi")



    val tweets = TwitterUtils.createStream(ssc,None,filters)

    val englishTweets = tweets.filter(_.getLang() == "en")

    englishTweets

  }

}
