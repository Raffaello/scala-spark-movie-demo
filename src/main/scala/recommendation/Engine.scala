package recommendation

import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Engine {

  import com.typesafe.config.{Config, ConfigFactory}

  private implicit val conf: Config = ConfigFactory.load().getConfig("recommendation")

  def train(ratings: DataFrame)(implicit ss: SparkSession): MatrixFactorizationModel = {
    import ss.implicits._

    val model = new ALS()
      .setBlocks(conf.getInt("num-blocks"))
      .setNonnegative(conf.getBoolean("non-negative"))
      .setIterations(conf.getInt("iterations"))
      .setLambda(conf.getDouble("lambda"))
      .setRank(conf.getInt("rank"))
      .setAlpha(conf.getDouble("alpha"))
      .setImplicitPrefs(conf.getBoolean("implicit-prefs"))

    if (!conf.getIsNull("seed")) model.setSeed(conf.getLong("seed"))
    // TODO: review the mapping using the df column names
    model.run(ratings.map(r => Rating(r.getInt(0), r.getInt(1), r.getInt(2).toDouble)).rdd)
  }

  def buildALSModel(ratings: DataFrame): ALSModel = ModelBuilder.buildALSModel(ratings)

  def save(model: MatrixFactorizationModel)(implicit sc: SparkContext): Unit = {
    model.save(sc, conf.getString("store-path"))
  }

  def load()(implicit sc: SparkContext): MatrixFactorizationModel = {
    MatrixFactorizationModel.load(sc, conf.getString("store-path"))
  }
}
