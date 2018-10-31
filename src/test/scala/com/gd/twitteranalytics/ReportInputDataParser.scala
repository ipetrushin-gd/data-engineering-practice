package com.gd.twitteranalytics

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.gd.twitteranalytics.reports.ReportInputDataParser._
import com.google.gson.Gson
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.{BeforeAndAfter, FreeSpec}

class ReportInputDataParserTest extends FreeSpec with DataFrameSuiteBase with BeforeAndAfter{

  val savePath = "twitter-analytics/sampleTest"

  after{
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(savePath),true)
  }

  "ReportInputDataParser " - {
    "getPayloadStatusAsDataset " - {
      "should convert JSON attributes to Dataframe with fields " in {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val twitterUser = TweetStatus(User("California","SFO","abc.com",false),"45456465")
        val gson = new Gson
        val userAsJson = gson.toJson(twitterUser)
        val userAsDataFrame = Seq(TweetJsonStatus(userAsJson)).toDF

        userAsDataFrame.write.parquet(savePath)
        val tweetsDataFrame = getPayloadStatusAsDataset(spark,savePath)
       assert("California",tweetsDataFrame.select("user.location").map(_.getString(0)).collect()(0))
      }
    }
    "getUserIdAndLocation " - {
      "should filter only User Location and User ID columns from presented DF " in {
        val sqlCtx = sqlContext
        import sqlCtx.implicits._

        val schema = List(
          StructField("location", StringType, false),
          StructField("id", StringType, false)
        )
        val twitterUser = TweetStatus(User("CA","SFO","abc.com",false),"45456465")
        val path = "twitter-analytics/sampleTest"
        val gson = new Gson
        val userAsJson = gson.toJson(twitterUser)

        val userAsDataFrame = Seq(TweetJsonStatus(userAsJson)).toDF
        userAsDataFrame.write.parquet(path)
        val tweetsDataFrame = getPayloadStatusAsDataset(spark,path)

        val filteredDF = getUserIdAndLocation(spark,tweetsDataFrame)
        assert(schema,filteredDF.schema)
      }
    }
  }
}
case class TweetJsonStatus(payload:String)
case class TweetStatus(user:User,id:String)
case class User(location:String,city:String,url:String,reTweeted:Boolean)