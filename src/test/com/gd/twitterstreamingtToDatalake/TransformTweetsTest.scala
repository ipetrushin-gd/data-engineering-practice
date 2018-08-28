package com.gd.twitterstreamingtToDatalake

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.{FunSuite, WordSpec}

import scala.collection.mutable
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

class TransformTweetsTest extends FunSuite with StreamingSuiteBase {

  conf.setAppName("twitterStreaming").setMaster("local[2]")

  val ssc = new StreamingContext(conf, Seconds(5))

  val lines: mutable.Queue[RDD[twitter4j.Status]] = mutable.Queue[RDD[twitter4j.Status]]()

  val dstream: InputDStream[Status] = ssc.queueStream(lines)
  lines += sc.makeRDD(Seq(twitter4j.Status,"To be or #not to be." ,"That is the #question."))

    test("Filter only words Starting with #")  {
      val inputTweet = List(List("this is #firstHash"), List("this is #secondHash"), List("this is #thirdHash"))
      val expected = List("#firstHash"), List("#secondHash"), List("#thirdHash"))

      testOperation(dstream, TransformTweets.getText _, expected, ordered = false)

    }

  test("really simple transformation") {
    val input = List(List("hi"), List("hi there you"), List("bye"))
    val expected = List(List("hi"), List("hi", "there", "you"), List("bye"))
    testOperation(input, TransformTweets.tokenize _, expected, ordered = false)
  }

}
