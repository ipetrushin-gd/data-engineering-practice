package com.gd.twitterstreamingtToDatalake

import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TransformTweets {

  def getText(englishTweets: DStream[Status]): DStream[String] = {
    val text = englishTweets.flatMap(x => x.getText.split(" "))

    text
  }

  def getHashTags(text: DStream[String]): DStream[String] = {
    val hashTags = text.filter(_.startsWith("#"))

    hashTags
  }

  // This is the sample operation we are testing
  def tokenize(f: DStream[String]): DStream[String] = {
    f.flatMap(_.split(" "))
  }
}
