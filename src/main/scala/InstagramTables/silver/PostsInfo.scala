package InstagramTables.silver

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.TimestampType

object PostsInfo {

    def extractPostInfoTable (instaData : DataFrame, spark: SparkSession) : DataFrame = {
      import spark.implicits._
      //val instaData = spark.read.option("multiline",true).json("/FileStore/tables/philCoutinho.json")
      val explodedGraphImages = instaData.select(explode($"GraphImages").as("GraphImages"))
      val postsData = explodedGraphImages.select(
        'GraphImages.getItem("taken_at_timestamp") as 'created_time,
        'GraphImages.getItem("is_video") as 'is_video,
        'GraphImages.getItem("location") as 'location,
        'GraphImages.getItem("username") as 'username,
        'GraphImages.getItem("display_url") as 'display_url
      )
      postsData

    }
  }



