package com.gd.twitterstreamingtToDatalake

import org.apache.log4j.{Level, Logger}


object StreamingTweetsJob extends SparkSessionWrapper {

  //Set the Logger Level

  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val englishTweets = tweetsIngestion.getTweets

    val hashTags = transformTweets.getText(englishTweets)







    hashTags .saveAsTextFiles("tweets","json")


    ssc.start()
    ssc.awaitTermination()








  }
}
