package models

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import utils.data_ingestion.FromCSV

object Rating extends FromCSV {
  private val customSchema = StructType(Array(
    StructField("user_id", IntegerType, true),
    StructField("movie_id", IntegerType, true),
    StructField("rating", IntegerType, true),
    StructField("timestamp", IntegerType, true)
  ))

  def readCSV(path: String)(implicit sqlContext: SparkSession): DataFrame = read(path, this.customSchema, "\t")
}
