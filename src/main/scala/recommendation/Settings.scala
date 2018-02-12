package recommendation

import com.typesafe.config.{Config, ConfigFactory}

class Settings(config: Config) {

  config.checkValid(ConfigFactory.defaultReference(), "recommendation")
  val trainingWeight = config.getDouble("recommendation.training-weight")
  require(trainingWeight > 0.0 && trainingWeight < 1.0)

  def this() {
    this(ConfigFactory.load())
  }
}
