import models.{Movie, Rating, User}
import utils.Charts.{MovieAges, Ratings, UserAges}
import utils.SparkSessionLocalMovieApp

object Charts extends App with SparkSessionLocalMovieApp {

  val userDf = User.readCSV("ml-100k/u.user")
  val movieDf = Movie.readCSV("ml-100k/u.item")
  val ratingDf = Rating.readCSV("ml-100k/u.data")

  UserAges.showChart(userDf)
  MovieAges.showChart(movieDf)
  Ratings.showUserRatings(ratingDf)
  Ratings.showRatings(ratingDf)

  sparkSession.close()
}
