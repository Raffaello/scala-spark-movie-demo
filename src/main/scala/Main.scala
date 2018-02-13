import models.{Movie, Rating, User}
import recommendation.Engine
import utils.SparkSessionLocalMovieApp

object Main extends App with SparkSessionLocalMovieApp {

  def getMovieBy(id: Int): String = {
    val movie = movieDf.filter(r => r(0) == id)
    if (movie.count() != 1) throw new IllegalArgumentException

    movie.collect()(0).getString(1)
  }

  val userDf = User.readCSV("ml-100k/u.user")
  val movieDf = Movie.readCSV("ml-100k/u.item")
  val ratingDf = Rating.readCSV("ml-100k/u.data")

  println(userDf.first())
  println(movieDf.first())
  println(ratingDf.first())

  // TODO: move to the train part and later here just load the model.
//  val model = Engine.train(ratingDf)
  val model = Engine.load()
  val userId = 789
  val K = 10
  val topKRecs = model.recommendProducts(userId, K)

  for(r <- topKRecs) {
    printf("%d - %s - %f\n", r.user, getMovieBy(r.product), r.rating)
  }

  sparkSession.close()
}
