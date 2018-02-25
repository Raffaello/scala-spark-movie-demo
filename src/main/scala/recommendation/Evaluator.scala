package recommendation

import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

private[recommendation] object Evaluator {

  def MSE(model: MatrixFactorizationModel, ratings: RDD[Rating]): Double = {
    def square(x: Double): Double = x*x
    val predictions = model
      .predict(ratings.map{ case Rating(u, p, _) => (u, p)})
      .map{ case Rating(u, p, r) => ((u, p), r)}

    ratings
      .map{ case Rating(u, p, r) =>((u, p), r) }
      .join(predictions)
      .map{ case (_ , (actual, predicted)) => square(actual - predicted) }
      .mean()
  }

  def RMSE(model: MatrixFactorizationModel, ratings: RDD[Rating]): Double = {
    math.sqrt(MSE(model, ratings))
  }
}
