import recommendation.Engine
import models.{Movie, Rating, User}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import utils.SparkSessionLocalMovieApp

object Main extends App with SparkSessionLocalMovieApp {

  val userDf = User.readCSV("ml-100k/u.user")
  val movieDf = Movie.readCSV("ml-100k/u.item")
  val ratingDf = Rating.readCSV("ml-100k/u.data")

  println(userDf.first())
  println(movieDf.first())
  println(ratingDf.first())

  // TODO: move to the train part and later here just load the model.
//  val alsModel = Engine.buildALSModel(ratingDf)
  //  Engine.saveModel(alsModel)

  val userId = 789
  val K = 10
  val topKRecs =  10

  sparkSession.close()
}
