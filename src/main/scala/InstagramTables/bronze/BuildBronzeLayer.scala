package InstagramTables.bronze

import org.apache.spark

object BuildBronzeLayer {
  def main (args: Array[String]) = {

    val BronzeLayer = spark.read.parquet("BronzeData.parquet")
  }

}
