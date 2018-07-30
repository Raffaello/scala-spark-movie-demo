import models.{Rating => MRating}
import org.apache.spark.mllib.recommendation.{MatrixFactorizationModel, Rating}
import recommendation.Engine
import utils.SparkSessionLocalMovieApp

object IterativeTrain extends App with SparkSessionLocalMovieApp {

  override def main(args: Array[String]) = {
    val ratingRDD = Engine.convert(MRating.readCSV("ml-100k/u.data"))

    // missing cross validation
    val Array(training, test) = ratingRDD.randomSplit(Array(0.8, 0.2))

    val numBlocks: Int = -1
    val nonNegative: Boolean = true
    val implicitPrefs: Boolean = false

    var bestModel: MatrixFactorizationModel = null
    var bestRmse = Double.MaxValue
    var bestIter = 20
    var bestLambda = 0.01
    var bestRank = 40
    var bestAlpha = 20.0

    def trainTest(i: Int, l: Double, r: Int, a: Double) = {
      val model = Engine.explicitTrain(training, numBlocks, nonNegative, i, l, r, a, implicitPrefs)
      val rmse = Engine.evaluator(model, ratingRDD, "RMSE")

      print(s"[i=$i - r=$r - l=$l - a=$a]")
      if (rmse < bestRmse) {
        bestModel = model
        bestRmse = rmse
        bestIter = i
        bestRank = r
        bestLambda = l
        bestAlpha = a

        println(s" new RMSE found : $bestRmse")
      } else {
        println()
      }
    }

    for {r <- bestRank to 40 by 2} {
      trainTest(bestIter, bestLambda, r, bestAlpha)
    }
//    for {a <- bestAlpha to 40.0 by 2} {
//      trainTest(bestIter, bestLambda, bestRank, a)
//    }
//      for { l <- bestLambda to 0.01 by 0.02} { trainTest(bestIter, l,bestRank,bestAlpha) }
//    for {i <- bestIter to 25 by 15} {
//      trainTest(i, bestLambda, bestRank, bestAlpha)
//    }

    println("BestModel values:")
    println(s"RMSE: $bestRmse")
    println(s"Iter: $bestIter")
    println(s"Rank: $bestRank")
    println(s"Lambda: $bestLambda")
    println(s"Alpha: $bestAlpha")

    Engine.save(bestModel)
    sparkSession.close()
  }
}
