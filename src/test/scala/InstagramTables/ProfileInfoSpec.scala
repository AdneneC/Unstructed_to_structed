package InstagramTables

import InstagramTables.silver.PostsInfo.extractPostInfoTable
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


case class RawData(
                    GraphProfileImages: String,
                    graphImages: Array[GraphImages]
                  )

case class GraphImages(comments: comments,
                       comments_disabled: Boolean,
                       dimensions: Dimensions,
                       display_url: String,
                       is_video: Boolean,
                       taken_at_timestamp: Long,
                       id: String,
                       location: String,
                       username: String)

case class comments(data: Array[data])

case class Dimensions(height: Long, width: Long)

case class data(created_at: Long, id: String, owner: Owner, text: String)

case class Owner(id: String, profile_pic_url: String, username: String)

case class Result(created_at: Long,
                  is_video: Boolean,
                  post_id: String,
                  location: String,
                  username: String,
                  display_url: String
                 )

class ProfileInfoSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("table-extractor")
    .getOrCreate()

  import spark.implicits._

  "extractPostInfoTable" should "extract profile info dataframe from raw data" in {
    Given("raw data ")
    val rawData = Seq(
      RawData(
        "String",
        Array(GraphImages(
          comments = comments(Array(data(created_at = 75L, id = "458", Owner(id = "String", profile_pic_url = "String", username = "String"), text ="text"))),
          comments_disabled = true,
          dimensions = Dimensions(height = 45, width = 35),
          display_url = "String",
          is_video = false,
          taken_at_timestamp = 75L,
          id = "String",
          location = "String",
          username = "String"
        ))
      )
    ).toDF()
    When("extractPostInfoTable is invoked")
    val result = extractPostInfoTable(rawData, spark)
    Then("profile info dataframe should be extracted from raw data")
    val expectedResult = Seq(
      Result(created_at = 75L,
        is_video =  false,
        post_id = "String",
        location = "String",
        username =  "String",
        display_url =  "String"
      )
    ).toDF()
    result.collect() should contain theSameElementsAs (expectedResult.collect())

  }

}
