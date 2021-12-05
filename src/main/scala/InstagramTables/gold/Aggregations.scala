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
    val likesCountPerPost =spark.sql (
      """
        |SELECT created_time , likes_count
        |FROM postsInfo
        |ORDER BY created_at asc
        |""".stripMargin
    )
    likesCountPerPost
  }

  def getCommentsCountPerPost (PostsInfo : DataFrame, spark : SparkSession):DataFrame = {
    val CommentsCountPerPost =spark.sql (
      """
        |SELECT created_time , comments_count
        |FROM postsInfo
        |ORDER BY created_at asc
        |""".stripMargin
    )
    CommentsCountPerPost
  }

  def getMostActiveUsers (CommentsInfo : DataFrame, spark : SparkSession):DataFrame ={
    val  MostActiveUsers = spark.sql (
      """
        |select owner_id, username, count(owner_id)
        |from commentsInfo
        |group by username, owner_id
        |order by count(owner_id) desc
        |""".stripMargin
    )
    MostActiveUsers
  }
}
//averageLikesAndAverageCommentsPerProfile.write.format("parquet").save("averageLikesAndAverageCommentsPerProfileTable")
