package InstagramTables

import InstagramTables.silver.CommentsInfo.extractCommentsInfoTable
import org.scalatest.GivenWhenThen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession


case class Komments(created_at : Long,
                  data_id : String,
                  owner_id : String,
                  owner_profile_pic_url : String,
                  username : String,
                  text : String
                 )

class CommentsInfoSpec extends AnyFlatSpec with Matchers with GivenWhenThen {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("table-extractor")
    .getOrCreate()

  import spark.implicits._

  "extractCommentsInfoTable" should "extract comments info dataframe from raw data" in {
    Given("the raw data")
    val rawData = Seq(
      RawData(
        GraphProfileImages(created_time = 75L, Info( biography = "String", followers_count = 77L, following_count = 77L, full_name = "String"),username = "String"),
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
    When ("extractCommentsInfoTable is invoked")
    val result = extractCommentsInfoTable(rawData, spark)
    Then ("comments info dataframe should be extracted from raw data")
    val expectedResult = Seq(
      Komments(created_at = 75L,
      data_id = "458",
      owner_id = "String",
      owner_profile_pic_url = "String",
      username = "String",
      text = "text"
    )
    ).toDF()
    result.collect() should contain theSameElementsAs (expectedResult.collect())
  }
}
