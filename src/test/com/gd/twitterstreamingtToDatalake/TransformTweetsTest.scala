package com.gd.twitterstreamingtToDatalake

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.{FunSuite}
import twitter4j.Status
import org.scalamock.scalatest.MockFactory

class TransformTweetsTest extends FunSuite with StreamingSuiteBase with MockFactory {

  test("Should Retreive Only Text from the Tweets Status") {
    val mockedStatus: Status = mock[twitter4j.Status]
    (mockedStatus.getText _).expects().returning("This is the #Sample #text for #test purpose")

    val input = List(List(mockedStatus))
    val expected = List(List("This is the #Sample #text for #test purpose"))
    testOperation(input, TransformTweets.getText _, expected, ordered = false)
  }

  test("Should Filter the HashTags from the Text Messages") {
    val input = List(List("#firstHash", "11", "22"), List("this", "is", "#secondHash"), List("#thirdHash", "44", "55"))
    val expected = List(List("#firstHash"), List("#secondHash"), List("#thirdHash"))

    testOperation(input, TransformTweets.getHashTags _, expected, ordered = false)
  }

  test("Sample Transformation to check the StreamingTestSuite") {
    val input = List(List("hi"), List("hi there you"), List("bye"))
    val expected = List(List("hi"), List("hi", "there", "you"), List("bye"))
    testOperation(input, TransformTweets.tokenize _, expected, ordered = false)
  }
}
