package com.gd.twitteranalytics

import com.gd.twitteranalytics.TwitterReportingJob.log
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount}

object TwitterReportGenerator {

  def getReportWithSqlProcessing(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    log.debug("=======> Stage 3: Twitter Report via SQL Start.... ")
    tweetStatusDf.createOrReplaceTempView("status")
    val query = "select location,id,count(id) as frequency from status group by location,id having frequency >= 10"
    spark.sql(query).withColumn("Date",lit(current_date)).createOrReplaceTempView("filteredTable")

    spark.sql("select location, id, Date from (select *, row_number() " +
      "OVER (PARTITION BY location ORDER BY frequency DESC) as rn  FROM filteredTable) tmp where rn <= 5")
  }

  def getReportWithDataFrameProcessing(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    log.debug("=======> Stage 4: Twitter Report via SQL Start.... ")
    import spark.implicits._

    val aggregatedDF  = tweetStatusDf.select($"location",$"id").
                  groupBy("location","id") .
                  agg(count($"id")as "frequency").where(count($"id") geq 10).
                  withColumn("Date",lit(current_date))

    val byBucket = Window.partitionBy($"location").orderBy($"frequency".desc)
    aggregatedDF.withColumn("rn", row_number.over(byBucket)).
            where($"rn" <= 5).drop("rn").drop("frequency")
  }

  def getReportWithDataSetProcessing(spark:SparkSession,tweetStatusDf:DataFrame):DataFrame = {
    log.debug("=======> Stage 5: Twitter Report via SQL Start.... ")
    import spark.implicits._

    val encoder = Encoders.product[TwitterStatus]
    val tweetDs:Dataset[TwitterStatus] = tweetStatusDf.as[TwitterStatus](encoder)
    val aggregatedDF = tweetDs.groupByKey(x=> TwitterStatus(x.location,x.id))
                        .agg(typedCount[TwitterStatus](_.id).name("frequency"))
                        .where($"frequency" geq 10)
                        .map{case( (TwitterStatus(location, id)),frequency) => Report(location,id,frequency)}

    val byBucket = Window.partitionBy($"location").orderBy($"frequency".desc)

    // This is attempt to get report via Typed Dataset --> But getting error Unable to find encoder for type stored in Dataset
   /* val reportViaTypedDataset = sortedDf.groupByKey(_.location).mapValues(List(_)).mapValues(_.sortBy(_.frequency).take(5)).
      mapGroups{case(k,iter)=> iter.map(x => x.toDF.as[Report])}*/

    aggregatedDF.withColumn("Date",lit(current_date)).
              withColumn("rn",row_number.over(byBucket)).
              where($"rn" <=5).drop("rn").drop("frequency")
  }
}

case class TwitterStatus(location:String,id:Long)
case class Report(location:String,id:Long,frequency:Long)