package com.gd.twitteranalytics

import com.gd.twitteranalytics.StreamingTweetsJob.log
import com.gd.twitteranalytics.util.ReadTwitterConf
import com.typesafe.config.ConfigException

object ConfigValidator {

  def isConfValid(keys: Array[String]): Boolean = {
    if (keys.length < 4) {
      log.error("Number of keys should be at least four! Please login with all keys !")
      log.error("Usage: TwitterData <ConsumerKey> <ConsumerSecret> <accessToken> <accessTokenSecret> " +
        "[<filters (If Any)>]")
      false
    }
    else  true
  }

  @throws[ConfigException]
  def validateTwitterAuth = {
      Array(ReadTwitterConf.CONSUMER_KEY, ReadTwitterConf.CONSUMER_SECRET, ReadTwitterConf.ACCESS_TOKEN, ReadTwitterConf.ACCESS_SECRET)
    }
}