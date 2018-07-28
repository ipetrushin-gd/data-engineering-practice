package com.gd.twitterstreamingtToDatalake



import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}




object TwitterStreamingDriver extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {

    if(args.length != 4){
      System.err.println("Usage <API key> <API secret key> <Access token> <Access token secret>")
      System.exit(1);
    }

    if (!Logger.getRootLogger.getAllAppenders.hasMoreElements) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)

    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")

   /* if (spark.contains("data-engineering-practice")) {
      spark.setAppName("data-engineering-practice-twitterStreaming")
    }*/

    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream = TwitterUtils.createStream(ssc,None)

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val schema = new StructType()
      .add(StructField("tweet", StringType, true))


    val df_hashTags = sparkConf.createDataFrame(hashTags, schema)

    df_hashTags.show()

    df_hashTags.write.format("parquet").save("hdfs path")

    ssc.start()
    ssc.awaitTermination()
  }
}
