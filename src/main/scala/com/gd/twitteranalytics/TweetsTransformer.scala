package com.gd.twitteranalytics

import java.util.{Date => JavaDate}
import java.sql.Timestamp
import com.gd.twitteranalytics.util.AppConfigReader
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.date_format
import twitter4j.Status
import org.apache.spark.rdd.RDD

object TweetsTransformer{

  def getDateAndText(englishTweets: DStream[Status]) = {
    englishTweets.map(x => (x.getCreatedAt,x.getText))
  }

  def convertRddToDataFrame(pairRdd:RDD[(JavaDate,String)])= {
    val spark = SparkSession.builder.config(pairRdd.sparkContext.getConf).getOrCreate
    import spark.implicits._

    pairRdd.map(tweets => Tweet(new Timestamp(tweets._1.getTime),tweets._2)).toDF
  }

  def updateDateColFormat(inputDataFrame:DataFrame,dateCol:String) = {
    val updatedDateFormat = date_format(inputDataFrame(dateCol),
                                                      AppConfigReader.OutputDateFormat)
    inputDataFrame.withColumn(dateCol,updatedDateFormat)
  }
}