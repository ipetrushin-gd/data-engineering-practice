package com.gd.twitterstreamingtToDatalake

import com.holdenkarau.spark.testing.StreamingSuiteBase
import org.scalatest.{FunSuite, WordSpec}

import twitter4j.Status
import org.scalamock.scalatest.MockFactory

class TransformTweetsTest extends FunSuite with StreamingSuiteBase with MockFactory{

  val mockedStatus:Status = mock[MockStatus]

  var input = new List[mockedStatus]

    test("Filter only words Starting with #")  {
      val input = List(List("#firstHash","11","22"), List("this","is","#secondHash"), List("#thirdHash","44","55"))
      val expected = List(List("#firstHash"),List("#secondHash"),List("#thirdHash"))

      testOperation(input, TransformTweets.getHashTags _, expected, ordered = false)
    }

  test("really simple transformation") {
    val input = List(List("hi"), List("hi there you"), List("bye"))
    val expected = List(List("hi"), List("hi", "there", "you"), List("bye"))
    testOperation(input, TransformTweets.tokenize _, expected, ordered = false)
  }



}
