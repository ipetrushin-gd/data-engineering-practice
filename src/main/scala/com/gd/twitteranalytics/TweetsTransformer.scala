package com.gd.twitteranalytics

import java.util.{Date => JavaDate}
import java.sql.Timestamp

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.date_format
import twitter4j.Status
import org.apache.spark.rdd.RDD

object TweetsTransformer{

  def getCreationDateAndStatus(englishTweets: DStream[Status]): DStream[(JavaDate,String)] = {
    val mapper = new ObjectMapper
    englishTweets.map(x => (x.getCreatedAt,mapper.writeValueAsString(x)))
  }

  def convertRddToDataFrame(pairRdd:RDD[(JavaDate,String)]) : DataFrame= {
    val spark = SparkSession.builder.config(pairRdd.sparkContext.getConf).getOrCreate
    import spark.implicits._

    pairRdd.map(tweets => Tweet(new Timestamp(tweets._1.getTime),tweets._2)).toDF
  }

  def updateDateColFormat(inputDataFrame:DataFrame,dateCol:String) : DataFrame = {
    val updatedDateFormat = date_format(inputDataFrame(dateCol),"YYYYMMdd")
    inputDataFrame.withColumn(dateCol,updatedDateFormat)
  }
}