package com.gd.twitteranalytics

import com.gd.twitteranalytics.StreamingTweetsJob.log
import com.gd.twitteranalytics.util.AppConfigReader

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

  def validateTwitterAuth = {
      Array(AppConfigReader.ConsumerKey, AppConfigReader.ConsumerSecret, AppConfigReader.AccessSecret, AppConfigReader.AccessToken)
    }
}