package com.gd.twitterstreamingtToDatalake

import org.scalatest.FunSuite

class ConfigValidationTest extends FunSuite {

  test("Should validate configuration if number of arguments is four or more") {
    val input = Array("aa", "bb", "bb", "cc")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === false)
  }

  test("Should fail the validation with error Logs if number of arguments is less than 4 ie.3") {
    val input = Array("aa", "bb", "bb")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }

  test("Should fail the validation with error logs if number of arguments is less than 4 ie.2") {
    val input = Array("aa", "bb")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }

  test("Should fail the validation with error Logs if number of arguments is less than 4 ie. 1") {
    val input = Array("aa")
    assert(StreamingTweetsJob.validateConfiguraiton(input) === true)
  }
}