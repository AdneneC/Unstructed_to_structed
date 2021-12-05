package InstagramTables.bronze

import org.apache.spark.sql.SparkSession

object BuildBronzeLayer {
  def main (args: Array[String]) = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Bronze")
      .getOrCreate()

    val BronzeLayer = spark.read.parquet("BronzeData.parquet")
  }

}
