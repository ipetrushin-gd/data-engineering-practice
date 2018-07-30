package com.gd.twitterstreamingtToDatalake





import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.DataFrame




object TwitterStreamingDriver extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
//if the number of args is more or less than 4 it will exit. accepts only 4
    if(args.length != 4){
      System.err.println("Usage <API key> <API secret key> <Access token> <Access token secret>")
      System.exit(1);
    }
//turn on logging
    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }
//authentication keys
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

//creating spark context
    val sparkConf = new SparkConf().setAppName("data-engineering-practice-twitterStreaming")

    /* if (spark.contains("data-engineering-practice")) {
       spark.setAppName("data-engineering-practice-twitterStreaming")
     }*/
//creating streaming context
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc,None)
//making flat and getting only hashtags
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
//defining schema for using while converting rdd to dataframe
    val schema = new StructType()
      .add(StructField("tweet", StringType, true))

//convertng rdd to dataframe
    val df_hashTags = sparkConf.createDataFrame(hashTags, schema)
//adding timestamp as new column
    val df_hashTags_timestamp = df_hashTags.withColumn("timestamp", lit(System.currentTimeMillis()))

//    printing dataframe to screen
    df_hashTags.show()
//saving dataframe on hdfs with timestamp as partition and saving in parquet format
    df_hashTags.write.partitionBy("timestamp").format("parquet").save("hdfs path")

    ssc.start()
    ssc.awaitTermination()
  }
}

