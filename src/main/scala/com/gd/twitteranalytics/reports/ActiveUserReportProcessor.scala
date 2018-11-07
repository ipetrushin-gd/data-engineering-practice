package com.gd.twitteranalytics.reports

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount}
import org.apache.spark.sql.functions.{count,lit, row_number}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object ActiveUserReportProcessor {

  def getReportWithSqlProcessing(spark: SparkSession, tweetStatusDf: DataFrame,reportDate:String): DataFrame = {
    tweetStatusDf.createOrReplaceTempView("status")
    val activeUsersIdAndLocation = """SELECT
                                        location,
                                        id,
                                        COUNT(id) AS frequency
                                      FROM status
                                      GROUP BY location,
                                               id
                                      HAVING frequency >= 10"""
    spark
      .sql(activeUsersIdAndLocation)
      .withColumn("date", lit(reportDate))
      .createOrReplaceTempView("activeUsers")

    spark.sql("""SELECT
                    location,
                    id,
                    date
                FROM (SELECT
                    *,
                    ROW_NUMBER()
                    OVER (PARTITION BY location ORDER BY frequency DESC) AS rowNum
                    FROM activeUsers) groupSortedByFreq
                WHERE rowNum <= 5""")
  }

  def getReportWithDataFrameProcessing(spark: SparkSession, tweetStatusDf: DataFrame,reportDate:String): DataFrame = {
    import spark.implicits._

    val activeUsers = tweetStatusDf
                      .select($"location", $"id")
                      .groupBy("location", "id")
                      .agg(count($"id") as "frequency")
                      .where(count($"id") geq 10)
                      .withColumn("date", lit(reportDate))

    val byBucket = Window.partitionBy($"location").orderBy($"frequency".desc)
    activeUsers.withColumn("rn", row_number.over(byBucket))
               .where($"rn" <= 5).drop("rn").drop("frequency")
  }

  def getReportWithDataSetProcessing(spark: SparkSession, tweetStatusDf: DataFrame,reportDate:String):DataFrame = {
    import spark.implicits._
    implicit val encoderForStatus = Encoders.product[TwitterStatus]
    implicit val encoderForReport = Encoders.product[Report]
    val tweetDs: Dataset[TwitterStatus] = tweetStatusDf.as[TwitterStatus](encoderForStatus)

    val activeUsers = tweetDs.groupByKey(x => TwitterStatus(x.location, x.id))
                              .agg(typedCount[TwitterStatus](_.id).name("frequency"))
                              .where($"frequency" geq 10)
                              .map { case (TwitterStatus(location, id), frequency) => Report(location, id, frequency) }

    activeUsers.groupByKey(_.location)
        .mapGroups{case(key,report)=> report.toList.sortBy(-_.frequency).take(5)}
        .flatMap(list =>list)
        .drop("frequency").withColumn("date", lit(reportDate))
  }
}
case class TwitterStatus(location:String,id:Long)
case class Report(location:String,id:Long,frequency:Long)
