package recommendation

import org.apache.spark.SparkContext
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
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

//  def cosSim(model: MatrixFactorizationModel): Double = {
//      val v1 = Vectors.dense(model.productFeatures.lookup(1).head)
//      model.productFeatures.lookup(1)
//  }

  def localCosineSimilarity(v1: Vector, v2: Vector): Double = {
    BDV(v1.toArray).dot(BDV(v2.toArray)) / (Vectors.norm(v1, 2) * Vectors.norm(v2, 2))
  }

  def localCosineSimilarity(vFeatures: RDD[(Int, Array[Double])], v: Vector, k: Int): Array[(Int, Double)] = {
    vFeatures
      .map{ case (i, a) => (i, localCosineSimilarity(v, Vectors.dense(a))) }
      .top(k)(Ordering.by(_._2))
  }

  /**
    * TODO: use RowMatrix to compute
    * @return
    */
  def cosineSimiliarity = ???
}
