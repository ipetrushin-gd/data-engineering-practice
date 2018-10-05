package com.gd.twitteranalytics.util

import com.typesafe.config.ConfigException
import org.scalatest.FreeSpec

class ApplicationConfigReaderTest extends FreeSpec {

  "AppConfigReader" - {
    "getAppConfigurables function" - {
      "should throw ConfigException if Application configurations are missing" in {
        val exceptionCause = "No configuration setting found for key "
        val thrown = intercept[ConfigException] {
          AppConfigReader.getAppConfigurables
        }
        assert(thrown.getMessage.contains(exceptionCause))
      }
    }
    "getTwitterAuthKeys function" - {
      "should throw ConfigException if Twitter Authentication keys are missing" in {
        val exceptionCause = "No configuration setting found for key "
        val thrown = intercept[ConfigException] {
          AppConfigReader.getTwitterAuthKeys
        }
        assert(thrown.getMessage.contains(exceptionCause))
      }
    }
  }
}
