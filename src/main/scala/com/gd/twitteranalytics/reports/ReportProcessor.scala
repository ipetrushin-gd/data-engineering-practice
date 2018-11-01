package com.gd.twitteranalytics.reports

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount}
import org.apache.spark.sql.functions.{count, current_date, lit, row_number}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object ReportProcessor {

  def getReportWithSqlProcessing(spark: SparkSession, tweetStatusDf: DataFrame): DataFrame = {
    tweetStatusDf.createOrReplaceTempView("status")
    val activeUsersIdAndLocation = """SELECT
                                        location,
                                        id,
                                        COUNT(id) AS frequency
                                      FROM status
                                      GROUP BY location,
                                               id
                                      HAVING frequency >= 10"""
    spark.sql(activeUsersIdAndLocation).withColumn("date", lit(current_date)).createOrReplaceTempView("activeUsers")

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

  def getReportWithDataFrameProcessing(spark: SparkSession, tweetStatusDf: DataFrame): DataFrame = {
    import spark.implicits._

    val activeUsers = tweetStatusDf.select($"location", $"id").
      groupBy("location", "id").
      agg(count($"id") as "frequency").where(count($"id") geq 10).
      withColumn("date", lit(current_date))

    val byBucket = Window.partitionBy($"location").orderBy($"frequency".desc)
    activeUsers.withColumn("rn", row_number.over(byBucket)).
      where($"rn" <= 5).drop("rn").drop("frequency")
  }

  def getReportWithDataSetProcessing(spark: SparkSession, tweetStatusDf: DataFrame):DataFrame = {
    import spark.implicits._
    implicit val encoderForStatus = Encoders.product[TwitterStatus]
    implicit val encoderForReport = Encoders.product[Report]
    val tweetDs: Dataset[TwitterStatus] = tweetStatusDf.as[TwitterStatus](encoderForStatus)

    val activeUsers = tweetDs.groupByKey(x => TwitterStatus(x.location, x.id))
      .agg(typedCount[TwitterStatus](_.id).name("frequency"))
      .where($"frequency" geq 10)
      .map { case (TwitterStatus(location, id), frequency) => Report(location, id, frequency) }

    activeUsers.groupByKey(_.location)
      .mapValues(List(_)).mapValues(_.sortBy(x => -x.frequency).take(5))
      .flatMapGroups{case(value,iter) => iter.flatten}.
      drop("frequency").withColumn("date", lit(current_date))
  }
}
case class TwitterStatus(location:String,id:Long)
case class Report(location:String,id:Long,frequency:Long)
