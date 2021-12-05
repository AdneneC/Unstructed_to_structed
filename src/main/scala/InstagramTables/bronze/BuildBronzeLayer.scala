package InstagramTables.bronze

import org.apache.spark

object BuildBronzeLayer {
  def main (args: Array[String]) = {

    spark.read.parquet("BronzeData.parquet")
  }

}
