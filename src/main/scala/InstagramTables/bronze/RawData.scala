package InstagramTables.bronze

import org.apache.spark.sql.{SparkSession}

object RawData {
  def bronzeData (spark: SparkSession) = {

    val bronzeRawData = spark.read.option("multiline",true).json("phil.coutinho.json")
    bronzeRawData.write.format("parquet").save("rawDataBronze.parquet")
  }
}
