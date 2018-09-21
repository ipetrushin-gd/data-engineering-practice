package com.gd.twitteranalytics

import java.util.{Date => JavaDate}
import java.sql.Timestamp

import TweetsDataSave.saveOutputToHdfs
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.date_format
import twitter4j.Status
import com.gd.twitteranalytics.util._
import org.apache.spark.rdd.RDD

@SerialVersionUID(112L)
object TransformTweets extends Serializable {

  def getDateAndText(englishTweets: DStream[Status]) = {
    englishTweets.map(x => (x.getCreatedAt,x.getText))
  }

  def processTweetsInfo(tweetsInfo: DStream[(JavaDate,String)])={
    tweetsInfo.foreachRDD{ rdd =>
      var tweetsDataFrame = getOutputAsDataFrame(rdd)
      tweetsDataFrame = updateDateColFormat(tweetsDataFrame)
      saveOutputToHdfs(tweetsDataFrame)
    }
  }

  def getOutputAsDataFrame(pairRdd:RDD[(JavaDate,String)])= {
    val spark = SparkSessionSingleton.getInstance(pairRdd.sparkContext.getConf)
    import spark.implicits._

    pairRdd.map(tweets => Tweets(new Timestamp(tweets._1.getTime),tweets._2)).toDF
  }

  def updateDateColFormat(inputDataFrame:DataFrame) = {

    val updatedDateFormat = date_format(inputDataFrame(TweetsConstant.PARTITION_COLUMN),
                                                      TweetsConstant.OUTPUT_DATE_FORMAT)
    inputDataFrame.withColumn(TweetsConstant.PARTITION_COLUMN,updatedDateFormat)
  }
}

object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}