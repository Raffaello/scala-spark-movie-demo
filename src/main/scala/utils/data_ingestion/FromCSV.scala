package utils.data_ingestion

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait FromCSV {

  protected def read(path: String, customSchema: StructType)(implicit sqlContext: SparkSession): DataFrame = {
    sqlContext.read.format("com.databricks.spark.csv")
      .option("delimiter", "|").schema(customSchema).load(path)
  }
}
