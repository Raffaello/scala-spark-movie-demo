package models

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.data_ingestion.FromCSV

object User extends FromCSV {

  private val customSchema = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("age", IntegerType, true),
    StructField("gender", StringType, true),
    StructField("occupation", StringType, true),
    StructField("zipCode", StringType, true)
  ))

  def readCSV(path: String)(implicit sqlContext: SparkSession): DataFrame = read(path, this.customSchema)
}
