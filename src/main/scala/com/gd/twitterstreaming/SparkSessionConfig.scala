package com.gd.twitterstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkSessionConfig {
  //TODO: Add Spark Configuration via command line

  val conf = new SparkConf()
  conf.setAppName("twitterStreaming").setMaster("local")

  val ssc = new StreamingContext(conf, Seconds(5))
}