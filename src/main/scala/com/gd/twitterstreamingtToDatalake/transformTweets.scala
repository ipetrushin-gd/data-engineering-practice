package com.gd.twitterstreamingtToDatalake

import org.apache.spark.streaming.dstream.DStream
import twitter4j.Status

object transformTweets {

  def getText(englishTweets: DStream[Status]): DStream[String] = {

    val hashTags = englishTweets.flatMap(x => x.getText.split(" ").filter(_.startsWith("#")))

    hashTags
  }
}
