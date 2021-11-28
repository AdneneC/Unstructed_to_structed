package InstagramTables

import InstagramTables.silver.ProfileInfo.extractProfileInfoTable
import org.apache.spark.sql.SparkSession
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class ResultProfile (created_time : Long,
                          username : String,
                          biography : String,
                          followers_count : Long,
                          following_count : Long,
                          full_name : String)

class ProfileInfoSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("table-extractor")
    .getOrCreate()

  import spark.implicits._

  "extractProfileInfoTable" should "extract profile info dataframe from raw data" in {
    Given("the raw data")
    val rawData = Seq(
      RawData(
        GraphProfileImages(created_time = 75L, Info( biography = "String", followers_count = 77L, following_count = 77L, full_name = "String"),username = "String"),
        Array(GraphImages(
          comments = comments(Array(data(created_at = 75L, id = "458", Owner(id = "String", profile_pic_url = "String", username = "String"), text = "text"))),
          comments_disabled = true,
          dimensions = Dimensions(height = 45, width = 35),
          display_url = "String",
          is_video = false,
          taken_at_timestamp = 75L,
          id = "String",
          location = "String",
          username = "String"
        )
        ))).toDF()
    When("extractProfileInfoTable is invoked")
    val result = extractProfileInfoTable(rawData, spark)
    Then("Profile info dataframe should be extracted from raw data")
    val expectedResult = Seq(
      ResultProfile(created_time = 75L,
        username = "String",
        biography = "String",
        followers_count = 77L,
        following_count = 77L,
        full_name = "String"
      )
    ).toDF()
    result.collect() should contain theSameElementsAs (expectedResult.collect())

  }
  }
