package com.gd.twitteranalytics.util

import com.typesafe.config.ConfigException
import org.scalatest.FreeSpec

class AppConfigReaderTest extends FreeSpec {

  "Methods tests" - {
    "getAppConfigurables" - {
      "should throw ConfigException error if Application configurations are missing" in {
        val exceptionCause = "No configuration setting found for key "
        val thrown = intercept[ConfigException] {
          AppConfigReader.getAppConfigurables
        }
        assert(thrown.getMessage.contains(exceptionCause))
      }
    }
    "getTwitterAuthKeys" - {
      "should throw ConfigException error if Twitter Authentication keys are missing" in {
        val exceptionCause = "No configuration setting found for key "
        val thrown = intercept[ConfigException] {
          AppConfigReader.getTwitterAuthKeys
        }
        assert(thrown.getMessage.contains(exceptionCause))
      }
    }
  }
}
