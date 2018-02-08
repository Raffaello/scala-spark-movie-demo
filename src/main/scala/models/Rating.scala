package models

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.data_ingestion.FromCSV

object Rating extends FromCSV {
  private val customSchema = StructType(Array(
    StructField("user_id", StringType, true),
    StructField("rating_id", StringType, true),
    StructField("rating", StringType, true),
    StructField("timestamp", StringType, true)
  ))

  def readCSV(path: String)(implicit sqlContext: SparkSession): DataFrame = read(path, this.customSchema, "t")

}
