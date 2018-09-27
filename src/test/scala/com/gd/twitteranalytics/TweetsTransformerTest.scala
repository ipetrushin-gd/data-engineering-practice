package twitteranalytics

import java.sql.Timestamp
import java.util.Date

import com.gd.twitteranalytics.TweetsTransformer
import com.holdenkarau.spark.testing.{DataFrameSuiteBase, StreamingSuiteBase}
import org.scalatest.FreeSpec
import twitter4j.{TwitterObjectFactory}
import org.scalamock.scalatest.MockFactory

import scala.io.Source

class TweetsTransformerTest extends FreeSpec with StreamingSuiteBase with MockFactory  with DataFrameSuiteBase {

  "TransformTweets" - {
    val constDate = new Date("Wed Sep 26 10:37:20 PDT 2018")
    val text = "This is the #Sample #text for #test purpose"
    val origDate = new Timestamp(constDate.getTime)

    "getDateAndText function" - {
    "when presented with a complete tweet status" - {
      "should retrieve date and text from the tweets status" in {
        val rawJson = Source.fromURL(getClass.getResource("/tweetStatus.txt")).getLines.mkString
        val tweetStatus = TwitterObjectFactory.createStatus(rawJson)
        val input = List(List(tweetStatus))
        val expected = List(List((constDate,text)))
        testOperation(input,TweetsTransformer.getDateAndText,expected,ordered = false)
      }
    }
  }
    "getOutputAsDataFrame function" - {
      "when presented with an RDD of Date and String" - {
        "should generate a Dataframe of columns event_date[TimeStamp] and text[String]" in {
          val sqlCtx = sqlContext
          import sqlCtx.implicits._

          val expectedDf = Seq((origDate,text),(origDate,text)).toDF("event_date","text")
          val inputRdd = sc.parallelize(Seq((constDate, text), (constDate, text)))
          val generatedDf = TweetsTransformer.convertRddToDataFrame(inputRdd)
          assertDataFrameEquals(expectedDf,generatedDf)
        }
      }
    }
    "updateDateColFormat function" - {
      "when presented with a DataFrame of format YYYY/MM/DD HH:MM:SS" -{
        "should update the format to YYYYMMDD HH.MM" in {
          val sqlCtx = sqlContext
          import sqlCtx.implicits._

          val sourceDf = Seq((origDate,text),(origDate,text)).toDF("event_date","text")
          val generatedDf = TweetsTransformer.updateDateColFormat(sourceDf,"event_date")
          val dateFromDf = generatedDf.select("event_date").first.getString(0)
          assert("20180926 10.37" === dateFromDf)
        }
      }
    }
  }
}