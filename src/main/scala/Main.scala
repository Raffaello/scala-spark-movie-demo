import models.{Movie, Rating => MRating, User}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.recommendation.Rating
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
  val ratingDf = MRating.readCSV("ml-100k/u.data")
  val ratingRDD = Engine.convert(ratingDf)

  println(userDf.first())
  println(movieDf.first())
  println(ratingDf.first())

  val model = Engine.load()
  val userId = 789
  val K = 10
  val topKRecs = model.recommendProducts(userId, K)

  println("Recommendation (UserId - MovieId - MovieTitle - Rating")
  topKRecs.foreach(r => printf("%d - (%d) %s - %f\n", r.user, r.product, getMovieBy(r.product), r.rating))
  val Array(r1, r2) = topKRecs.take(2).map(r => r.product)
  val v1 = Vectors.dense(model.productFeatures.lookup(r1).head)
  val v2 = Vectors.dense(model.productFeatures.lookup(r2).head)

  val m1 = getMovieBy(r1)
  println(s"Cosine Similarity for Movie $m1 -> $m1")
  println(Engine.localCosineSimilarity(v1, v1))
  println(s"Cosine Similarity for Movie $m1 -> ${getMovieBy(r2)}")
  println(Engine.localCosineSimilarity(v1, v2))

  println(s"Cosine Similarity for Movies compare to $m1, top $K")
  Engine.localCosineSimilarity(model.productFeatures, v1, K)
    .foreach{ case (i, d) => printf("%d - %f (%s)\n", i, d, getMovieBy(i))}

  println(s"Cosine similarity for users compare to $userId, top $K")
  println(Engine.localCosineSimilarity(
    model.userFeatures,
    Vectors.dense(model.userFeatures.lookup(userId).head),
    K).mkString("\n"))

  println()
  println("Evaluation:")
  println(s"RMSE: ${Engine.evaluator(model, ratingRDD, "RMSE")}")
//  val actualUserMovies = movieDf.filter(r => r(0) == userId).map(r => r.getInt(0)).collect()
  println(s"APK, ${Engine.evaluate(userId, movieDf, topKRecs, K, "APK")}")

  val regMetrics = Engine.evaluate(model, ratingRDD)
  println("regMetrics:")
  println(s"RMSE = ${regMetrics.rootMeanSquaredError}")
  println(s"MSE  = ${regMetrics.meanSquaredError}")
  println(s"MAE  = ${regMetrics.meanAbsoluteError}")
  println(s"EV   = ${regMetrics.explainedVariance}")
  println(s"r2   = ${regMetrics.r2}")

  sparkSession.close()
}
