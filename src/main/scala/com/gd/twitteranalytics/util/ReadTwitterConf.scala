package com.gd.twitteranalytics.util

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

@throws[ConfigException]
object ReadTwitterConf{


  val config:Config = ConfigFactory.load("twitterAnalytics.conf")

  val PARTITION_COLUMN = config.getString("application.partition_name")
  val SAVEPATH = config.getString("application.save_path")
  val OUTPUT_DATE_FORMAT = config.getString("application.date_format")
  val TWEETS_LANG_FILTER = config.getString("application.filter")
  val HASHTAGS = config.getString("application.hashtags")

  val APPNAME = config.getString("application.app_name")
  val MASTERURL = config.getString("application.masterUrl")

  val CONSUMER_KEY = config.getString("configKeys.twitter4j.oauth.consumerKey")
  val CONSUMER_SECRET = config.getString("configKeys.twitter4j.oauth.consumerSecret")
  val ACCESS_TOKEN = config.getString("configKeys.twitter4j.oauth.accessToken")
  val ACCESS_SECRET = config.getString("configKeys.twitter4j.oauth.accessTokenSecret")
}
