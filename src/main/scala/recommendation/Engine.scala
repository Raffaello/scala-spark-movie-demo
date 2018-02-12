package recommendation

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.DataFrame

object Engine {

  import com.typesafe.config.{Config, ConfigFactory}

  private val conf: Config = ConfigFactory.load().getConfig("recommendation")

  def buildALSModel(ratings: DataFrame): ALSModel = {

    val trainingWeight = conf.getDouble("training-weight")
    val Array(training, test) = ratings.randomSplit(Array(trainingWeight, 1.0 - trainingWeight))
    val alsConf = conf.getConfig("ASL")
    val als = new ALS()
      .setMaxIter(alsConf.getInt("max-iter"))
      .setRegParam(alsConf.getDouble("reg-param"))
      .setUserCol(alsConf.getString("col-names.user-id"))
      .setItemCol(alsConf.getString("col-names.movie-id"))
      .setRatingCol(alsConf.getString("col-names.rating"))
      .setImplicitPrefs(alsConf.getBoolean("implicit-pgit srefs"))

    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy(alsConf.getString("cold-start-strategy"))

    val predictions = model.transform(test)

    val evalConf = conf.getConfig("evaluator")
    val evaluator = new RegressionEvaluator()
      .setMetricName(evalConf.getString("metric-name"))
      .setLabelCol(evalConf.getString("label-col"))
      .setPredictionCol(evalConf.getString("prediction-col"))

    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

    model
  }
}
