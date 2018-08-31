package com.gd.twitterstreamingtToDatalake

import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TransformTweets {

  def getText(englishTweets: DStream[Status]): DStream[String] = {
    val text = englishTweets.map(_.getText)

    text
  }

  def getHashTags(text: DStream[String]): DStream[String] = {
    val hashTags = text.flatMap(_.split(" ")).filter(_.startsWith("#"))

    hashTags
  }

}