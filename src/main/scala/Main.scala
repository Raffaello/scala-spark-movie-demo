import models.{Movie, User}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.Charts.{MovieAges, UserAges}

object Main extends App {

  val spConfig = (new SparkConf).setMaster("local").setAppName("moviesApp")
  implicit val sparkSession: SparkSession = SparkSession
    .builder().appName("moviesApp").config(spConfig).getOrCreate()

  val userDf = User.readCSV("ml-100k/u.user")
  val movieDf = Movie.readCSV("ml-100k/u.item")

//  val rdd = userDf.rdd
  println(userDf.first())
  println(movieDf.first())

  UserAges.showChart(userDf)
  MovieAges.showChart(movieDf)

  sparkSession.close()
}

