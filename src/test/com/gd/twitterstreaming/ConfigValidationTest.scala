package com.gd.twitterstreaming

import org.scalatest.FreeSpec

class ConfigValidationTest extends FreeSpec {

  "ConfigValidator"- {
    "when invoked with 4 or more arguments"- {
      "should pass validation" in {
        val input = Array("aa", "bb", "cc", "dd")
        assert(ConfigValidator.isConfValid(input) === true)
      }
    }
    "when invoked with less than 4 arguments that is 3"- {
      "should fail vaildation with error logs" in {
        val input = Array("aa", "bb", "bb")
        assert(ConfigValidator.isConfValid(input) === false)
      }
      "should fail vaildation with error logs that is 2" in {
        val input = Array("aa", "bb")
        assert(ConfigValidator.isConfValid(input) === false)
      }
      "should fail vaildation with error logs that is 1" in {
        val input = Array("aa")
        assert(ConfigValidator.isConfValid(input) === false)
      }
      "should fail vaildation with error logs that is none" in {
        val input = Array[String]()
        assert(ConfigValidator.isConfValid(input) === false)
      }
    }
  }
}