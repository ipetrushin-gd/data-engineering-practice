package com.gd.twitteranalytics.reports

import java.io.{File, FileInputStream}
import java.util.Properties

import com.gd.twitteranalytics.util.AppConfigReader

//TODO Check the external configuration feasability with deployment to Oozie workflow manager..
object ExternalConfigLoader {

  val reportProperties = new Properties
  val externalConfigPath = AppConfigReader.getReportConfigurables(1)
  val jarPath = new File(getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
  val reportExternalConfigPath = jarPath.getParentFile.getAbsolutePath + "/" + externalConfigPath
  val input = new FileInputStream(reportExternalConfigPath )

  reportProperties.load(input)

  def getEventDateForReportCreation:String = {
    reportProperties.getProperty("eventDateOfDataForReport")
  }
}
