package models

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.data_ingestion.FromCSV

object Movie extends FromCSV {

  private val customSchema = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("date", StringType, true),
    StructField("url", StringType, true)
  ))

  def readCSV(path: String)(implicit sqlContext: SparkSession): DataFrame = read(path, this.customSchema)
}
