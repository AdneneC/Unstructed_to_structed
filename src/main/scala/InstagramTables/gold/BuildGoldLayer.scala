package InstagramTables.gold

import InstagramTables.gold.Aggregations.{getAverageLikesAndAverageCommentsPerProfile, getCommentsCountPerPost, getLikesCountLikesPerPost, getMostActiveUsers}
import InstagramTables.silver.PostsInfo
import org.apache.spark.sql.{DataFrame, SparkSession}


object BuildGoldLayer {
  def main (args: Array[String]) = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Gold")
      .getOrCreate()

    val silverCommentsInfo = spark.read.parquet("SilverCommentsTable.parquet")
    val silverPostInfo = spark.read.parquet("SilverPostInfoTable.parquet")
    val silverProfileInfo = spark.read.parquet("SilverProfileInfoTable.parquet")

    silverPostInfo.createOrReplaceTempView("PostInfoTable")
    silverCommentsInfo.createOrReplaceTempView("CommentsTable")
    silverProfileInfo.createOrReplaceTempView("ProfileInfoTable")

    getAverageLikesAndAverageCommentsPerProfile("averageLikesAndAverageCommentsPerProfile.parquet",spark)
    getLikesCountLikesPerPost("LikesCountLikesPerPost.parquet",spark)
    getCommentsCountPerPost("CommentsCountLikesPerPost.parquet",spark)
    getMostActiveUsers("UsersWithMostComments.parquet",spark)


  }

}
