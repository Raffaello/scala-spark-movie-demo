package recommendation

import breeze.linalg.DenseMatrix
import breeze.util.JavaArrayOps
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

private[recommendation] object Evaluator {

  /**
    * Utils method
    * @param model
    * @param ratings
    * @return RDD of ((user, prediction), (actual, predicted))
    */
  def ratesAndPred(model: MatrixFactorizationModel, ratings: RDD[Rating]): RDD[((Int, Int), (Double, Double))] = {
    val predictions = model
      .predict(ratings.map{ case Rating(u, p, _) => (u, p)})
      .map{ case Rating(u, p, r) => ((u, p), r)}

    ratings
      .map{ case Rating(u, p, r) =>((u, p), r) }
      .join(predictions)
  }

  /**
    * @deprecated
    * @param model
    * @param ratings
    * @return
    */
  def MSE(model: MatrixFactorizationModel, ratings: RDD[Rating]): Double = {
    def square(x: Double): Double = x*x
    ratesAndPred(model, ratings)
      .map{ case (_ , (actual, predicted)) => square(actual - predicted) }
      .mean()
  }

  /**
    * @deprecated
    * @param model
    * @param ratings
    * @return
    */
  def RMSE(model: MatrixFactorizationModel, ratings: RDD[Rating]): Double = {
    math.sqrt(MSE(model, ratings))
  }

  /**
    * Average Precision at K:
    * APK is a measure of the average relevance scores of a set of
    * the top-K documents presented in response to a query.
    *
    * @param actual
    * @param predicted
    * @param k
    * @return
    */
  def APK(actual: Seq[Int], predicted: Seq[Int], k: Int): Double = {
    if (actual.isEmpty) {
      1.0
    } else {
      var score = 0.0
      var numHits = 0

      for {
        (p, i) <- predicted.take(k).zipWithIndex
        if actual.contains(p)
      } {
        numHits += 1
        score += numHits.toDouble / (i.toDouble + 1.0)
      }

      score / math.min(actual.size, k).toDouble
    }
  }

  /**
    * TODO: use distributed matrix
    * Mean Average Precision at K
    * @return
    */
  def MAPK(imBroadcast: Broadcast[DenseMatrix[Double]], model: MatrixFactorizationModel, ratings: RDD[Rating], k: Int)
          (implicit sc: SparkContext): Double = {

    val allRecs = model.userFeatures.map { case (i, a) =>
      val userVector = JavaArrayOps.arrayDToDv(a)
      val scores = imBroadcast.value * userVector
      val sortedWithId = scores.data.zipWithIndex.sortBy(-_._1)
      val recommendIds = sortedWithId.map(_._2 + 1).toSeq
      (i, recommendIds)
    }

    val userProducts = ratings.map { case Rating(u, p, _) => (u, p) }
      .groupBy(_._1)

    allRecs.join(userProducts).map { case (_, (predicted, actualWithIds)) =>
      val actual = actualWithIds.map(_._2).toSeq
      APK(actual, predicted, k)
    }.mean()
  }
  /**
    * Local Sensitive Hashing
    * @return
    */
  def LSH(): Double = ???
}
