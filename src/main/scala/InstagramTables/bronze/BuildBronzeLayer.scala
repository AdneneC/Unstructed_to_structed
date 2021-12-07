package InstagramTables.bronze

import org.apache.spark.sql.SparkSession

object BuildBronzeLayer {
  def main (args: Array[String]) = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Bronze")
      .getOrCreate()

    val rawData = spark.read.option("multiLine", true).json("phil_coutinho-1.json")
    rawData.write.format("parquet").save("BronzeData.parquet")

  }

}

