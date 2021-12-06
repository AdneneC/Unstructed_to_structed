package InstagramTables.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object Aggregations {

  def getAverageLikesAndAverageCommentsPerProfile ( tableName : String ,spark: SparkSession) = {

  val query =
    """
      |select is_video, location  FROM PostsInfoTable
      |""".stripMargin

    executeAndPersistQuery(spark , query , tableName)

  }

  def getLikesCountLikesPerPost (tableName : String, spark : SparkSession) = {

    val query =
      """
        |SELECT created_time , likes_count
        |FROM postsInfo
        |ORDER BY created_at asc
        |""".stripMargin

    executeAndPersistQuery(spark , query , tableName)

  }

  def getCommentsCountPerPost (tableName : String, spark : SparkSession) = {

    val query =
      """
        |SELECT created_time , comments_count
        |FROM postsInfo
        |ORDER BY created_at asc
        |""".stripMargin

    executeAndPersistQuery(spark , query , tableName)
  }

  def getMostActiveUsers (tableName : String, spark : SparkSession) ={

    val  query =
      """
        |select owner_id, username, count(owner_id)
        |from commentsInfo
        |group by username, owner_id
        |order by count(owner_id) desc
        |""".stripMargin

    executeAndPersistQuery(spark , query , tableName)

  }

  def executeAndPersistQuery(spark : SparkSession , query : String , tableName : String) =
    spark.sql(query).write.mode("overwrite").parquet(tableName)
}
//averageLikesAndAverageCommentsPerProfile.write.format("parquet").save("averageLikesAndAverageCommentsPerProfileTable")
