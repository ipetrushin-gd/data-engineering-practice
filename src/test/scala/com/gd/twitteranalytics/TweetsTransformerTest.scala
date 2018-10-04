package twitteranalytics

import java.sql.Timestamp
import java.util.Date

import com.gd.twitteranalytics.TweetsTransformer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, StreamingSuiteBase}
import org.scalatest.FreeSpec
import twitter4j.TwitterObjectFactory

import com.fasterxml.jackson.databind.ObjectMapper

import scala.io.Source

class TweetsTransformerTest extends FreeSpec with StreamingSuiteBase with DataFrameSuiteBase {

  "TransformTweets" - {
    val creationTime = new Date("Wed Sep 26 10:37:20 PDT 2018")
    val text = "This is the #Sample #text for #test purpose"
    val eventDate = new Timestamp(creationTime.getTime)

    "getCreationDateAndStatus function" - {
    "when presented with a complete tweet status" - {
      "should retrieve date and status from tweets" in {
        val rawJson = Source.fromURL(getClass.getResource("/tweetStatus.json")).getLines.mkString
        val tweetStatus = TwitterObjectFactory.createStatus(rawJson)
        val tweetStatusAsJson = new ObjectMapper().writeValueAsString(tweetStatus)

        val input = List(List(tweetStatus))
        val expected = List(List((eventDate,tweetStatusAsJson)))
        testOperation(input,TweetsTransformer.getCreationDateAndStatus,expected,ordered = false)
      }
    }
  }
    "getOutputAsDataFrame function" - {
      "when presented with an RDD of Date and String" - {
        "should generate a Dataframe of columns event_date[TimeStamp] and payload[String]" in {
          val sqlCtx = sqlContext
          import sqlCtx.implicits._

          val expectedDf = Seq((eventDate,text),(eventDate,text)).toDF("event_date","payload")
          val inputRdd = sc.parallelize(Seq((creationTime, text), (creationTime, text)))
          val generatedDf = TweetsTransformer.convertRddToDataFrame(inputRdd)
          assertDataFrameEquals(expectedDf,generatedDf)
        }
      }
    }
    "updateDateColFormat function" - {
      "when presented with a DataFrame of format YYYY-MM-DD HH:MM:SS" -{
        "should update the format to YYYYMMDD" in {
          val sqlCtx = sqlContext
          import sqlCtx.implicits._

          val sourceDf = Seq((eventDate,text),(eventDate,text)).toDF("event_date","text")
          val generatedDf = TweetsTransformer.updateDateColFormat(sourceDf,"event_date")
          val dateFromDf = generatedDf.select("event_date").first.getString(0)
          assert("20180926" === dateFromDf)
        }
      }
    }
  }
}