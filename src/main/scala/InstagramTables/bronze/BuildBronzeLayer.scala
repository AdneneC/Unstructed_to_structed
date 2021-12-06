package InstagramTables.bronze

import org.apache.spark.sql.SparkSession

object BuildBronzeLayer {
  def main (args: Array[String]) = {

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Bronze")
      .getOrCreate()

    val rawData = spark.read.option("multiline",true).json("phil.coutinho.json")
    rawData.write.format("parquet").mode("overwrite").save("BronzeData.parquet")

    val BronzeLayer = spark.read.parquet("BronzeData.parquet")
  }

}

