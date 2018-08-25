package com.gd.twitterstreamingtToDatalake

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.{FunSuite, WordSpec}

class TransformTweetsTest extends WordSpec with StreamingSuiteBase {

  "TransformTweets" should {
    "Filter only words Starting with #" in {
      val inputTweet = List(List("this is #firstHash"), ("this is #secondHash"), ("this is #thirdHash"))
      val filteredTweet = List(List("#firstHash"), ("#secondHash"), ("#thirdHash"))

      testOperation(inputTweet, TransformTweets.getText _, filteredTweet, ordered = false)
    }

  }
}
