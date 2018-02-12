package recommendation

import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.DataFrame

object Engine {

  import com.typesafe.config.{Config, ConfigFactory}

  private implicit val conf: Config = ConfigFactory.load().getConfig("recommendation")

  def train(ratings: DataFrame): MatrixFactorizationModel = {
//    val trainingWeight = conf.getDouble("training-weight")
//    val Array(training, test) = ratings.randomSplit(Array(trainingWeight, 1.0 - trainingWeight))
    val alsConf = conf.getConfig("rdd.ASL")

    val model = new ALS()
      .setBlocks(alsConf.getInt("num-blocks"))
      .setNonnegative(alsConf.getBoolean("non-negative"))
      .setImplicitPrefs(alsConf.getBoolean("implicit-prefs"))
      .setIterations(alsConf.getInt("iterations"))
      .setLambda(alsConf.getDouble("lambda"))
      .setRank(alsConf.getInt("rank"))
      .setAlpha(alsConf.getDouble("alpha"))
      // TODO: review the mapping using the df column names
      .run(ratings.map(r => Rating(r.getInt(0), r.getInt(1), r.getFloat(2))).rdd)

    model
  }

  def buildALSModel(ratings: DataFrame): ALSModel = ModelBuilder.buildALSModel(ratings)

  def save(model: MatrixFactorizationModel)(implicit sc: SparkContext): Unit = {
    model.save(sc, conf.getString("ASL.store-path"))
  }

  def load()(implicit sc: SparkContext): MatrixFactorizationModel = {
    MatrixFactorizationModel.load(sc, conf.getString("ASL.store-path"))
  }
}
