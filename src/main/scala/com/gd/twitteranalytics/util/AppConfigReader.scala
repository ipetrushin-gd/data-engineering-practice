package com.gd.twitteranalytics.util

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

object AppConfigReader{
//TODO Define configurations via case class
  def getAppConfigurables : Array[String] = {

    val appConfig: Config = ConfigFactory.load("application.conf")
    val SavePath = appConfig.getString("application.save_path")
    val TweetsLangFilter = appConfig.getString("application.filter")
    val HashTags = appConfig.getString("application.hashtags")
      Array(SavePath,TweetsLangFilter,HashTags)
  }

  def getReportConfigurables ={

    val reportConfig: Config = ConfigFactory.load("application.conf")
    val SavePathForReports = reportConfig.getString("reporting.reportSavePath")
    val reportExternalConfigPath = reportConfig.getString("reporting.reportExternalConfigPath")
    Array(SavePathForReports,reportExternalConfigPath)
  }

  def getTwitterAuthKeys : Array[String] = {

    val twitterConfig: Config = ConfigFactory.load("twitterAuthKeys.conf")
    val ConsumerKey = twitterConfig.getString("configKeys.twitter4j.oauth.consumerKey")
    val ConsumerSecret = twitterConfig.getString("configKeys.twitter4j.oauth.consumerSecret")
    val AccessToken = twitterConfig.getString("configKeys.twitter4j.oauth.accessToken")
    val AccessSecret = twitterConfig.getString("configKeys.twitter4j.oauth.accessTokenSecret")
    Array(ConsumerKey,ConsumerSecret,AccessToken,AccessSecret)
  }

}