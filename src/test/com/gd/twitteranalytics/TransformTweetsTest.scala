package com.gd.twitteranalytics

import java.sql.Timestamp
import java.util.Date

import com.holdenkarau.spark.testing.{DataFrameSuiteBase, StreamingSuiteBase}
import org.scalatest.FreeSpec
import twitter4j.Status
import org.scalamock.scalatest.MockFactory

@SerialVersionUID(100L)
class TransformTweetsTest extends FreeSpec with Serializable with StreamingSuiteBase with MockFactory  with DataFrameSuiteBase {

  "TransformTweets" - {
    val constDate = new Date("Fri Sep 14 11:39:34 PDT 2018")
    val text = "This is the #Sample #text for #test purpose"
    val origDate = new Timestamp(constDate.getTime)

    "getDateAndText function" - {
    "when presented with a complete tweet status" - {
      "should retrieve date and text from the tweets status" in {
        val mockedStatus: Status = mock[twitter4j.Status]
        (mockedStatus.getText _).expects.returning(text)
        (mockedStatus.getCreatedAt _).expects.returning(constDate)

        val input = List(List(mockedStatus))
        val expected = List(List((constDate,text),(constDate,text)))
        testOperation(input,TransformTweets.getDateAndText _,expected,ordered = false)
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
          val generatedDf = TransformTweets.getOutputAsDataFrame(inputRdd)
          assertDataFrameEquals(expectedDf,generatedDf)
        }
      }
    }
    "updateDateColFormat function" - {
      "when presented with a DataFrame of format YYYY/MM/DD HH:MM:SS" -{
        "should update the format to YYYYMMDD HH.MM.SS" in {
          val sqlCtx = sqlContext
          import sqlCtx.implicits._

          val sourceDf = Seq((origDate,text),(origDate,text)).toDF("event_date","text")
          val generatedDf = TransformTweets.updateDateColFormat(sourceDf)
          val dateFromDf = generatedDf.select("event_date").first.getString(0)
          assert("20180914 11.39.34" === dateFromDf)
        }
      }
    }
  }
}