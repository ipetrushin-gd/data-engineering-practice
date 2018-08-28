package com.gd.twitterstreamingtToDatalake

import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object TransformTweets {

  def getText(englishTweets: DStream[Status]): DStream[String] = {
    val hashTags = englishTweets.flatMap(x => x.getText.split(" ").filter(_.startsWith("#")))

    hashTags
  }

  // This is the sample operation we are testing
  def tokenize(f: DStream[String]): DStream[String] = {
    f.flatMap(_.split(" "))
  }
}
