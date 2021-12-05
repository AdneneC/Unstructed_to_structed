package InstagramTables.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object Aggregations {
  def getAverageLikesAndAverageCommentsPerProfile (PostsInfo : DataFrame, spark: SparkSession): DataFrame = {
  PostsInfo.createOrReplaceTempView("PostsInfoTable")
  val averageLikesAndAverageCommentsPerProfile = spark.sql(
    """
      |select avg (likes_count) as avg_likes , avg ( comments_count ) as avg_comments  FROM PostsInfoTable
      |""".stripMargin
    )
    averageLikesAndAverageCommentsPerProfile
  }

  def getLikesCountLikesPerPost (PostsInfo : DataFrame, spark : SparkSession):DataFrame = {
    val countLikesPerPost =spark.sql (
      """
        |SELECT created_time , likes_count
        |FROM postsInfo
        |ORDER BY created_at asc
        |""".stripMargin
    )
    countLikesPerPost
  }

  def getLikesCountCommentsPerPost (PostsInfo : DataFrame, spark : SparkSession):DataFrame = {
    val countCommentsPerPost =spark.sql (
      """
        |SELECT created_time , comments_count
        |FROM postsInfo
        |ORDER BY created_at asc
        |""".stripMargin
    )
    countCommentsPerPost
  }

  def getUsersWithMostComments (CommentsInfo : DataFrame, spark : SparkSession):DataFrame ={
    val  usersWithMostComments = spark.sql (
      """
        |select owner_id, username, count(owner_id)
        |from commentsInfo
        |group by username, owner_id
        |order by count(owner_id) desc
        |""".stripMargin
    )
    usersWithMostComments
  }
}
//averageLikesAndAverageCommentsPerProfile.write.format("parquet").save("averageLikesAndAverageCommentsPerProfileTable")
