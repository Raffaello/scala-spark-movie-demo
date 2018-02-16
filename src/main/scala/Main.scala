import models.{Movie, Rating, User}
import org.apache.spark.mllib.linalg.Vectors
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

  val model = Engine.load()
  val userId = 789
  val K = 10
  val topKRecs = model.recommendProducts(userId, K)

  topKRecs.foreach(r => printf("%d - (%d) %s - %f\n", r.user, r.product, getMovieBy(r.product), r.rating))

  val v1 = Vectors.dense(model.productFeatures.lookup(1022).head)
  val v2 = Vectors.dense(model.productFeatures.lookup(1368).head)

  println(Engine.localCosineSimilarity(v1, v1))
  println(Engine.localCosineSimilarity(v1, v2))

  Engine.localCosineSimilarity(model.productFeatures, v1, K)
    .foreach{ case (i, d) => printf("%d - %f (%s)\n", i, d, getMovieBy(i))}

  println()
  println(Engine.localCosineSimilarity(
    model.userFeatures,
    Vectors.dense(model.userFeatures.lookup(userId).head),
    K).mkString("\n"))

  sparkSession.close()
}
