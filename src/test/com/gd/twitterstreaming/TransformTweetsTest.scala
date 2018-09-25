package com.gd.twitterstreaming

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.FreeSpec
import twitter4j.Status
import org.scalamock.scalatest.MockFactory

class TransformTweetsTest extends FreeSpec with StreamingSuiteBase with MockFactory {

  "TransformTweets" - {
    "getText function" - {
    "when presented with a complete tweet status" - {
      "should retrieve only text from the tweets status" in {
        val mockedStatus: Status = mock[twitter4j.Status]

        (mockedStatus.getText _).
          expects.
          returning("This is the #Sample #text for #test purpose")

        val input = List(List(mockedStatus))
        val expected = List(List("This is the #Sample #text for #test purpose"))
        testOperation(input, TransformTweets.getText _, expected, ordered = false)
      }
    }
  }
    "getHashTags function" -{
      "when presented with a stream of text containing hashtags" - {
        "should filter the hashtags from the test message" in {
          val input = List(List("#firstHash", "11", "22"), List("this", "is", "#secondHash"), List("#thirdHash", "44", "55"))
          val expected = List(List("#firstHash"), List("#secondHash"), List("#thirdHash"))

          testOperation(input, TransformTweets.getHashTags _, expected, ordered = false)
        }
      }
    }
  }
}
