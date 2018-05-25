import models.Rating
import recommendation.Engine
import utils.SparkSessionLocalMovieApp

object Training extends App with SparkSessionLocalMovieApp {

  val ratingDf = Rating.readCSV("ml-100k/u.data")
//  val alsModel = Engine.buildALSModel(ratingDf)

  //  TextProcessing.processTitles(movieDf)

  // TODO: missing split train,test (and cross validation)
  val model = Engine.train(ratingDf)
  Engine.save(model)

  sparkSession.close()
}
