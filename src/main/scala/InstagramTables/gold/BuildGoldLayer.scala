package InstagramTables.gold

import InstagramTables.gold.Aggregations.{getAverageLikesAndAverageCommentsPerProfile, getCommentsCountPerPost, getLikesCountLikesPerPost, getMostActiveUsers}
import org.apache.spark
import org.apache.spark.sql.SparkSession


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

    getAverageLikesAndAverageCommentsPerProfile.write.format("parquet").save("averageLikesAndAverageCommentsPerProfileTable")
    getLikesCountLikesPerPost.write.format("parquet").save("LikesCountLikesPerPost")
    getCommentsCountPerPost.write.format("parquet").save("CommentsCountLikesPerPost")
    getMostActiveUsers.write.format("parquet").save("UsersWithMostComments")


  }

}
