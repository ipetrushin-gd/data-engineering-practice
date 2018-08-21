package com.gd.twitterstreamingtToDatalake

object transformTweets {

  def getText(DStream[Status]) = {

  val hashTags = englishTweets.flatMap(x =>x.getText.split(" ").filter(_.startsWith("#")))

  hashTags
  }
}
