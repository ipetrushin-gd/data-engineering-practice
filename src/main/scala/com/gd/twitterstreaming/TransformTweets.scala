package com.gd.twitterstreaming

import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TransformTweets {

  def getText(englishTweets: DStream[Status]): DStream[String] = {
    englishTweets.map(_.getText)
  }

  def getHashTags(text: DStream[String]): DStream[String] = {
    text.flatMap(_.split(" ")).filter(_.startsWith("#"))
  }
}