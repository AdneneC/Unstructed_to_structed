package InstagramTables.silver

import InstagramTables.silver.PostsInfo.extractPostInfoTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.TimestampType

object CommentsInfo {
  def extractCommentsInfoTable(instaData: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val explodedData = instaData.select(explode($"GraphImages").as("GraphImages"))
    val explodedFinally = explodedData.select(explode($"GraphImages.comments.data"))
    val commentsData = explodedFinally.select(
      'col.getItem("created_at").cast(TimestampType) as 'comment_created_at,
      'col.getItem("id").cast("Long") as 'data_id,
      'col.getItem("owner").getItem("id") as 'owner_id,
      'col.getItem("owner").getItem("profile_pic_url") as 'owner_profile_pic_url,
      'col.getItem("owner").getItem("username") as 'username,
      'col.getItem("text") as 'text
    )

    commentsData
  }

}
