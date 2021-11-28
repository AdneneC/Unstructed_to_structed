package InstagramTables.bronze

import org.apache.spark.sql.SparkSession

object RawData {
  def readAndPersistRawData (spark: SparkSession, path: String): Unit = {

    val rawData = spark.read.option("multiline",true).json(path)
    rawData.write.format("parquet").save("BronzeData.parquet")
  }
}
