package InstagramTables.silver

import InstagramTables.bronze.{BuildBronzeLayer, RawData}
import org.apache.spark.sql.SparkSession

object BuildSilverLayer{
  def main (args: Array[String]) = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SilverLayer")
      .getOrCreate()

    val commentsTable = CommentsInfo.extractCommentsInfoTable(BuildBronzeLayer)
    commentsTable.write.mode("append").parquet("SilverCommentsTable.parquet")

    val postInfoTable = PostsInfo.extractPostInfoTable(BuildBronzeLayer)
    postInfoTable.write.mode("append").parquet("SilverPostInfoTable.parquet")

    val profileInfoTable = ProfileInfo.extractProfileInfoTable(BuildBronzeLayer)
    profileInfoTable.write.mode("append").parquet("SilverProfileInfoTable.parquet")

    val silverCommentsData = spark.read.parquet("SilverCommentsTable.parquet")
    val silverPostInfoTable = spark.read.parquet("SilverPostInfoTable.parquet")
    val silverProfileInfo = spark.read.parquet("SilverProfileInfoTable.parquet"
  }

}
