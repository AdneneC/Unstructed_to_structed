package InstagramTables.silver
import org.apache.spark.sql.{SparkSession,DataFrame}

object BuildSilverLayer{
  def main (args: Array[String]) = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Silver")
      .getOrCreate()

    val BronzeLayer = spark.read.option("multiLine", true).json("phil_coutinho-1.json")

    val commentsTable = CommentsInfo.extractCommentsInfoTable(BronzeLayer, spark)
    commentsTable.write.mode("append").parquet("SilverCommentsTable.parquet")

    val postInfoTable = PostsInfo.extractPostInfoTable(BronzeLayer,spark)
    postInfoTable.write.mode("append").parquet("SilverPostInfoTable.parquet")

    val profileInfoTable = ProfileInfo.extractProfileInfoTable(BronzeLayer, spark)
    profileInfoTable.write.mode("append").parquet("SilverProfileInfoTable.parquet")


  }

}
