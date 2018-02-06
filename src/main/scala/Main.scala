import models.User
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Main extends App {

  val spConfig = (new SparkConf).setMaster("local").setAppName("moviesApp")
  implicit val sparkSession: SparkSession = SparkSession
    .builder().appName("moviesApp").config(spConfig).getOrCreate()

  val userDf = User.readCSV("ml-100k/u.user")

//  val rdd = userDf.rdd
  println(userDf.first())
}

