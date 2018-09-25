package com.gd.twitterstreaming

import com.gd.twitterstreaming.StreamingTweetsJob.log

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
}