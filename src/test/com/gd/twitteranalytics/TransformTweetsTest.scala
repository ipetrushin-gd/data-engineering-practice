package com.gd.twitteranalytics

import java.sql.Timestamp
import java.util.Date

import com.holdenkarau.spark.testing.{DataFrameSuiteBase,StreamingSuiteBase}
import org.apache.spark.sql.types._
import org.scalatest.FreeSpec
import twitter4j.Status
import org.scalamock.scalatest.MockFactory

class TransformTweetsTest extends FreeSpec with StreamingSuiteBase with MockFactory  with DataFrameSuiteBase {

  "TransformTweets" - {
    "getDateAndText function" - {
    "when presented with a complete tweet status" - {
      "should retrieve date and text from the tweets status" in {
        val mockedStatus: Status = mock[twitter4j.Status]
        val constDate = new Date("Fri Sep 14 11:39:34 PDT 2018")

        (mockedStatus.getText _).expects.
                                returning("This is the #Sample #text for #test purpose")
        (mockedStatus.getCreatedAt _).expects.returning(constDate)

        val input = List(List(mockedStatus))
        val expected = List(List(constDate,"This is the #Sample #text for #test purpose"))
        testOperation(input,TransformTweets.getDateAndText,expected,ordered = false)
      }
    }
  }
    "getOutputAsDataFrame function" - {
      "when presented with an RDD of Date and String" - {
        "should generate a Dataframe of columns Date and Text" in {
          val constDate = new Date("Fri Sep 14 11:39:34 PDT 2018")
          val text = "This is the Test Phrase"
          val inputRdd = sc.parallelize(Seq((constDate, text), (constDate, text)))
          val schema = StructType(List(StructField("event_date", TimestampType), StructField("text", StringType)))
          val df = TransformTweets.getOutputAsDataFrame(inputRdd)

          assert(df.schema === schema)
        }
      }
    }
    "updateDateColFormat function" - {
      "when presented with a DataFrame of format YYYY/MM/DD HH:MM:SS" -{
        "should update the format to YYYYMMDD HH.MM.SS" in {
          val sqlCtx = sqlContext
          import sqlCtx.implicits._

          val constDate = new Timestamp(new Date("Fri Sep 17 10:46:11 PDT 2018").getTime)
          val text = "This is the Test Phrase"
          val sourceDF = Seq((constDate,text),(constDate,text)).toDF("event_date","text")
          val df = TransformTweets.updateDateColFormat(sourceDF)
          val dateOfDf = df.select("event_date").first.getString(0)

          assert("20180917 10.46.11" === dateOfDf)
        }
      }
    }
  }
}