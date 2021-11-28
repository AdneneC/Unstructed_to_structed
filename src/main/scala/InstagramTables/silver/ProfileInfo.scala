package InstagramTables.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.TimestampType

object ProfileInfo {
  def extractProfileInfoTable(instaData: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val ProfileData = instaData.select(
      'GraphProfileImages.getItem("created_time") as 'created_time,
      'GraphProfileImages.getItem("username") as 'username,
      'GraphProfileImages.getItem("info").getItem("biography") as 'biography,
      'GraphProfileImages.getItem("info").getItem("followers_count") as 'followers_count,
      'GraphProfileImages.getItem("info").getItem("following_count") as 'following_count,
      'GraphProfileImages.getItem("info").getItem("full_name") as 'full_name
    )

    ProfileData
  }
}
