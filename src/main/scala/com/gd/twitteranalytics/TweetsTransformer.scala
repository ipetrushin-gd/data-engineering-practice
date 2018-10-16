package com.gd.twitteranalytics

import java.util.{Date => JavaDate}
import java.sql.Date

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.to_date
import twitter4j.Status
import org.apache.spark.rdd.RDD

object TweetsTransformer{

  def getCreationDateAndStatus(englishTweets: DStream[Status]): DStream[(JavaDate,String)] = {
    val mapper = new ObjectMapper
    englishTweets.map(x => (x.getCreatedAt,mapper.writeValueAsString(x)))
  }

  def convertRddToDataFrame(pairRdd:RDD[(JavaDate,String)]) : DataFrame = {
    val spark = SparkSession.builder.config(pairRdd.sparkContext.getConf).getOrCreate
    import spark.implicits._

    pairRdd.map{case(date,payload) => Tweet(new Date(date.getTime),payload)}.toDF
  }

  def updateDateColFormat(inputDataFrame:DataFrame,dateCol:String): DataFrame = {
    val updatedDateFormat = to_date(inputDataFrame(dateCol),"yyyy-MM-dd")
    inputDataFrame.withColumn(dateCol,updatedDateFormat)
  }

  def convertDataFrameToDataSet(inputDataFrame:DataFrame): Dataset[Tweet] = {
    val encoder = org.apache.spark.sql.Encoders.product[Tweet]
    inputDataFrame.as[Tweet](encoder)
  }
}