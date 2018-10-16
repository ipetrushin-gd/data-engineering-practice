package twitteranalytics

import com.gd.twitteranalytics.ConfigValidator
import org.scalatest.FreeSpec

class ConfigValidationTest extends FreeSpec {

  "ConfigValidator isConfValid function"- {
    "when invoked with 4 or more arguments"- {
      "should pass validation" in {
        val input = Array("aa", "bb", "cc", "dd")
        assert(ConfigValidator.isConfValid(input) === true)
      }
    }
    "when invoked with less than 4 arguments that is 3"- {
      "should fail vaildation" in {
        val input = Array("aa", "bb", "bb")
        assert(ConfigValidator.isConfValid(input) === false)
      }
    }
    "when invoked with less than 4 arguments that is 2"- {
      "should fail vaildation" in {
        val input = Array("aa", "bb")
        assert(ConfigValidator.isConfValid(input) === false)
      }
    }
    "when invoked with less than 4 arguments that is 1"- {
      "should fail vaildation" in {
        val input = Array("aa")
        assert(ConfigValidator.isConfValid(input) === false)
      }
    }
    "when invoked with less than 4 arguments that is None"- {
      "should fail vaildation" in {
        val input = Array[String]()
        assert(ConfigValidator.isConfValid(input) === false)
      }
    }
  }
}