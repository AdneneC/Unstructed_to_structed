package InstagramTables.silver

import InstagramTables.bronze.{BuildBronzeLayer, RawData}
import org.apache.spark.sql.SparkSession

object BuildSilverLayer{
  def main (args: Array[String]) = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Silver")
      .getOrCreate()

    val commentsTable = CommentsInfo.extractCommentsInfoTable(BuildBronzeLayer)
    commentsTable.write.mode("append").parquet("SilverCommentsTable.parquet")

    val postInfoTable = PostsInfo.extractPostInfoTable(BuildBronzeLayer)
    postInfoTable.write.mode("append").parquet("SilverPostInfoTable.parquet")

    val profileInfoTable = ProfileInfo.extractProfileInfoTable(BuildBronzeLayer)
    profileInfoTable.write.mode("append").parquet("SilverProfileInfoTable.parquet")


  }

}
