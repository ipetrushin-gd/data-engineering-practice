package com.gd.twitterstreamingtToDatalake

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.FunSuite
import twitter4j.Status
import org.scalamock.scalatest.MockFactory

class TransformTweetsTest extends FunSuite with StreamingSuiteBase with MockFactory {

  test("Should retreive only text from the tweets status") {
    val mockedStatus: Status = mock[twitter4j.Status]
    (mockedStatus.getText _).expects().returning("This is the #Sample #text for #test purpose")

    val input = List(List(mockedStatus))
    val expected = List(List("This is the #Sample #text for #test purpose"))
    testOperation(input, TransformTweets.getText _, expected, ordered = false)
  }

  test("Should filter the hashTags from the text messages") {
    val input = List(List("#firstHash", "11", "22"), List("this", "is", "#secondHash"), List("#thirdHash", "44", "55"))
    val expected = List(List("#firstHash"), List("#secondHash"), List("#thirdHash"))

    testOperation(input, TransformTweets.getHashTags _, expected, ordered = false)
  }
}