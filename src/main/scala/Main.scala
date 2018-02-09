import models.{Movie, Rating, User}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.Charts.{MovieAges, RatingCount, UserAges}

object Main extends App {

  val spConfig = (new SparkConf).setMaster("local").setAppName("moviesApp")
  implicit val sparkSession: SparkSession = SparkSession
    .builder().appName("moviesApp").config(spConfig).getOrCreate()

  val userDf = User.readCSV("ml-100k/u.user")
  val movieDf = Movie.readCSV("ml-100k/u.item")
  val ratingDf = Rating.readCSV("ml-100k/u.data")

  println(userDf.first())
  println(movieDf.first())
  println(ratingDf.first())

  UserAges.showChart(userDf)
  MovieAges.showChart(movieDf)
  RatingCount.showChart(ratingDf)

  sparkSession.close()
}
