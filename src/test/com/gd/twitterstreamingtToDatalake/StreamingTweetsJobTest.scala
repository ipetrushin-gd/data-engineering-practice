package com.gd.twitterstreamingtToDatalake

import org.scalatest.FunSuite

class StreamingTweetsJobTest extends FunSuite {

  test("Should Validate Configuration if Number of Arguments is four or more") {
    val input = Array("aa", "bb", "bb", "cc")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === false)
  }

  test("Should Fail the Validation with Error Logs if Number of Arguments is less than 4 ie.3") {
    val input = Array("aa", "bb", "bb")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }

  test("Should Fail the Validation with Error Logs if Number of Arguments is less than 4 ie.2") {
    val input = Array("aa", "bb")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }

  test("Should Fail the Validation with Error Logs if Number of Arguments is less than 4 ie. 1") {
    val input = Array("aa")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }
}