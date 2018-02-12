package utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionLocalMovieApp {

  val spConfig: SparkConf = (new SparkConf).setMaster("local").setAppName("moviesApp")
  implicit val sparkSession: SparkSession = SparkSession
    .builder().appName("moviesApp").config(spConfig).getOrCreate()
}
