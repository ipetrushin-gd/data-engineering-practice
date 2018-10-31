package com.gd.twitteranalytics.reports

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount}
import org.apache.spark.sql.functions.{count, current_date, lit, row_number}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object ReportProcessor {

  def getReportWithSqlProcessing(spark: SparkSession, tweetStatusDf: DataFrame): DataFrame = {
    tweetStatusDf.createOrReplaceTempView("status")
    val query = "select location,id,count(id) as frequency from status group by location,id having frequency >= 10"
    spark.sql(query).withColumn("Date", lit(current_date)).createOrReplaceTempView("filteredTable")

    spark.sql("""select location, id, Date from (select *, row_number()
      OVER (PARTITION BY location ORDER BY frequency DESC) as rn  FROM filteredTable) tmp where rn <= 5""")
  }

  def getReportWithDataFrameProcessing(spark: SparkSession, tweetStatusDf: DataFrame): DataFrame = {
    import spark.implicits._

    val aggregatedDF = tweetStatusDf.select($"location", $"id").
      groupBy("location", "id").
      agg(count($"id") as "frequency").where(count($"id") geq 10).
      withColumn("Date", lit(current_date))

    val byBucket = Window.partitionBy($"location").orderBy($"frequency".desc)
    aggregatedDF.withColumn("rn", row_number.over(byBucket)).
      where($"rn" <= 5).drop("rn").drop("frequency")
  }

  def getReportWithDataSetProcessing(spark: SparkSession, tweetStatusDf: DataFrame):DataFrame = {
    import spark.implicits._
    implicit val encoderForStatus = Encoders.product[TwitterStatus]
    implicit val encoderForReport = Encoders.product[Report]
    val tweetDs: Dataset[TwitterStatus] = tweetStatusDf.as[TwitterStatus](encoderForStatus)

    val aggregatedDF = tweetDs.groupByKey(x => TwitterStatus(x.location, x.id))
      .agg(typedCount[TwitterStatus](_.id).name("frequency"))
      .where($"frequency" geq 10)
      .map { case (TwitterStatus(location, id), frequency) => Report(location, id, frequency) }

    aggregatedDF.groupByKey(_.location)
      .mapValues(List(_)).mapValues(_.sortBy(x => -x.frequency).take(5))
      .flatMapGroups{case(value,iter) => iter.flatten}.
      drop("frequency").withColumn("Date", lit(current_date))
  }
}

case class TwitterStatus(location:String,id:Long)
case class Report(location:String,id:Long,frequency:Long)
