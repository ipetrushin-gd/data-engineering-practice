package com.gd.twitterstreamingtToDatalake

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait SparkSessionWrapper {

  //Set the Spark Streaming Context

  val conf = new SparkConf()
  conf.setAppName("twitterStreaming").setMaster("local")

  val  ssc = new StreamingContext(conf,Seconds(5))

}
