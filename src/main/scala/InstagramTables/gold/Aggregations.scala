package InstagramTables.gold

import org.apache.spark.sql.{DataFrame, SparkSession}

object Aggregations {
def getAverageLikesAndAverageCommentsPerProfile (PostsInfo : DataFrame, spark: SparkSession): DataFrame = {
  PostsInfo.createOrReplaceTempView("PostsInfoTable")
  val averageLikesAndAverageCommentsPerProfile = spark.sql(
    """
      |select avg (likes_count) as avg_likes , avg ( comments_count ) as avg_comments  FROM PostsInfoTable
      |""".stripMargin)
  averageLikesAndAverageCommentsPerProfile
}
}
//averageLikesAndAverageCommentsPerProfile.write.format("parquet").save("averageLikesAndAverageCommentsPerProfileTable")
