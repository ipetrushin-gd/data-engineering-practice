package com.gd.twitterstreamingtToDatalake

import org.scalatest.FunSuite


class StreamingTweetsJobTest extends FunSuite {

  test("Correct Number of Arguments for Main") {
    val input = Array("aa", "bb", "bb", "cc")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === false)
  }

  test("Incorrect Number of Arguments for Main1") {
    val input = Array("aa", "bb", "bb")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }

  test("Incorrect Number of Arguments for Main2") {
    val input = Array("aa", "bb")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }

  test("Incorrect Number of Arguments for Main3") {
    val input = Array("aa")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }

}
