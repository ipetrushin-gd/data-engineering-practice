package com.gd.twitteranalytics.util

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

object AppConfigReader{

  val appConfig:Config = ConfigFactory.load("application.conf")
  val twitterConfig:Config = ConfigFactory.load("twitterAuthKeys.conf")

  val SavePath = appConfig.getString("application.save_path")
  val OutputDateFormat = appConfig.getString("application.date_format")
  val TweetslangFilter = appConfig.getString("application.filter")
  val HashTags = appConfig.getString("application.hashtags")

  val AppName = appConfig.getString("application.app_name")
  val MasterUrl = appConfig.getString("application.masterUrl")

  val ConsumerKey = twitterConfig.getString("configKeys.twitter4j.oauth.consumerKey")
  val ConsumerSecret = twitterConfig.getString("configKeys.twitter4j.oauth.consumerSecret")
  val AccessToken = twitterConfig.getString("configKeys.twitter4j.oauth.accessToken")
  val AccessSecret = twitterConfig.getString("configKeys.twitter4j.oauth.accessTokenSecret")
}